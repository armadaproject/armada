package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/xitongsys/parquet-go/parquet"

	v1 "k8s.io/api/core/v1"
)

const (
	maxManifestBytes     = 4 << 20
	maxPartitions        = 4096
	maxLogicalRowBytes   = 64 << 10
	maxRequirementsBytes = 16 << 10
	maxJobIDBytes        = 256
)

type workloadManifest struct {
	SchemaVersion        int                 `json:"schema_version"`
	ExecutionModel       string              `json:"execution_model"`
	DatasetID            string              `json:"dataset_id"`
	ConverterVersion     string              `json:"converter_version"`
	WorkEstimationMethod string              `json:"work_estimation_method"`
	QualityCounts        map[string]int64    `json:"quality_counts"`
	Partitions           []partitionManifest `json:"partitions"`
}

type partitionManifest struct {
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	Rows           int64  `json:"rows"`
	MinSubmittedUS *int64 `json:"min_submitted_at_us"`
	MaxSubmittedUS *int64 `json:"max_submitted_at_us"`
}

type workload struct {
	root     *os.Root
	manifest workloadManifest
	hash     string
}

func openWorkload(dir string) (*workload, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	w := &workload{root: root}
	if err := w.loadManifest(); err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return w, nil
}

func (w *workload) loadManifest() error {
	f, err := w.root.Open("manifest.json")
	if err != nil {
		return err
	}
	raw, readErr := readBounded(f, maxManifestBytes)
	if err := errors.Join(readErr, f.Close()); err != nil {
		return err
	}
	w.hash = fmt.Sprintf("%x", sha256.Sum256(raw))
	if err := strictJSON(raw, &w.manifest); err != nil {
		return fmt.Errorf("workload manifest: %w", err)
	}
	m := &w.manifest
	if m.SchemaVersion != 1 || m.ExecutionModel != "fixed-useful-work-v1" ||
		m.DatasetID == "" || m.ConverterVersion == "" || m.WorkEstimationMethod == "" ||
		m.QualityCounts == nil || m.Partitions == nil || len(m.Partitions) > maxPartitions {
		return fmt.Errorf("invalid workload manifest, expected schema 1 and execution model fixed-useful-work-v1")
	}
	for name, count := range m.QualityCounts {
		if name == "" || count < 0 {
			return fmt.Errorf("invalid quality count %q", name)
		}
	}
	paths := make(map[string]bool, len(m.Partitions))
	for _, p := range m.Partitions {
		digest, err := hex.DecodeString(p.SHA256)
		if !fs.ValidPath(p.Path) || !filepath.IsLocal(p.Path) || p.Path == "." ||
			filepath.Ext(p.Path) != ".parquet" || paths[p.Path] || err != nil || len(digest) != sha256.Size ||
			p.Rows < 0 || p.Rows > maxGroups*maxGroupRows {
			return fmt.Errorf("invalid workload partition %q", p.Path)
		}
		paths[p.Path] = true
		if p.Rows == 0 {
			if p.MinSubmittedUS != nil || p.MaxSubmittedUS != nil {
				return fmt.Errorf("empty partition %q must omit submission range", p.Path)
			}
		} else if p.MinSubmittedUS == nil || p.MaxSubmittedUS == nil || *p.MinSubmittedUS > *p.MaxSubmittedUS {
			return fmt.Errorf("partition %q must specify a valid submission range", p.Path)
		}
	}
	return nil
}

func (w *workload) openPartition(ctx context.Context, index int) (*partitionReader, error) {
	p := w.manifest.Partitions[index]
	f, err := w.root.Open(p.Path)
	if err != nil {
		return nil, err
	}
	part, err := readParquetPartition(ctx, f)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if part.footer.NumRows != p.Rows {
		return nil, errors.Join(fmt.Errorf("expected %d rows, footer reports %d", p.Rows, part.footer.NumRows), f.Close())
	}
	actual := sha256.New()
	if _, err := io.Copy(actual, contextReader{ctx, f}); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	expected, _ := hex.DecodeString(p.SHA256)
	if !bytes.Equal(actual.Sum(nil), expected) {
		return nil, errors.Join(errors.New("SHA-256 checksum differs from manifest"), f.Close())
	}
	return part, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(buf []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(buf)
}

type workloadStream struct {
	ctx       context.Context
	input     *workload
	index     int
	rowNumber int64
	part      *partitionReader
	previous  Job
}

func (s *workloadStream) close() error {
	if s.part == nil {
		return nil
	}
	f := s.part.file
	s.part = nil
	return f.Close()
}

func (s *workloadStream) next() (Job, error) {
	for s.index < len(s.input.manifest.Partitions) {
		if err := s.ctx.Err(); err != nil {
			return Job{}, err
		}
		p := s.input.manifest.Partitions[s.index]
		if s.part == nil {
			part, err := s.input.openPartition(s.ctx, s.index)
			if err != nil {
				return Job{}, fmt.Errorf("partition %q: %w", p.Path, err)
			}
			s.part, s.rowNumber = part, 0
		}
		raw, err := s.part.next(s.ctx)
		if err == io.EOF {
			if err := s.close(); err != nil {
				return Job{}, fmt.Errorf("close partition %q: %w", p.Path, err)
			}
			s.index++
			continue
		}
		if err != nil {
			return Job{}, fmt.Errorf("partition %q: %w", p.Path, err)
		}
		s.rowNumber++
		job, err := parseJob(raw)
		if err != nil {
			return Job{}, fmt.Errorf("partition %q row %d: %w", p.Path, s.rowNumber, err)
		}
		if (s.rowNumber == 1 && job.SubmittedUS != *p.MinSubmittedUS) ||
			(s.rowNumber == p.Rows && job.SubmittedUS != *p.MaxSubmittedUS) {
			return Job{}, fmt.Errorf("partition %q row %d disagrees with manifest submission range", p.Path, s.rowNumber)
		}
		if s.previous.ID != "" && (job.SubmittedUS < s.previous.SubmittedUS ||
			(job.SubmittedUS == s.previous.SubmittedUS && job.ID <= s.previous.ID)) {
			return Job{}, fmt.Errorf("partition %q row %d violates (submitted_at_us, logical_job_id) order", p.Path, s.rowNumber)
		}
		s.previous = Job{ID: job.ID, SubmittedUS: job.SubmittedUS}
		return job, nil
	}
	return Job{}, io.EOF
}

type requirements struct {
	Resources struct {
		Requests map[string]string `json:"requests"`
	} `json:"resourceRequirements"`
	NodeSelector map[string]string `json:"nodeSelector"`
	Affinity     *struct {
		Node *struct {
			Required *v1.NodeSelector `json:"requiredDuringSchedulingIgnoredDuringExecution"`
		} `json:"nodeAffinity"`
	} `json:"affinity"`
	Tolerations []v1.Toleration `json:"tolerations"`
}

func parseJob(row map[string]any) (Job, error) {
	var job Job
	var textBytes int
	for _, column := range workloadColumns {
		value := row[column.name]
		if value == nil {
			if column.required {
				return job, fmt.Errorf("required column %q is null", column.name)
			}
			continue
		}
		if column.kind == parquet.Type_BYTE_ARRAY {
			str, ok := value.(string)
			if !ok || !utf8.ValidString(str) {
				return job, fmt.Errorf("column %q must be UTF-8", column.name)
			}
			textBytes += len(str)
		} else if _, ok := value.(int64); !ok {
			return job, fmt.Errorf("column %q must be an integer", column.name)
		}
	}
	if textBytes+8*len(row) > maxLogicalRowBytes {
		return job, fmt.Errorf("logical row exceeds %d bytes", maxLogicalRowBytes)
	}
	job.ID = row["logical_job_id"].(string)
	job.SubmittedUS = row["submitted_at_us"].(int64)
	job.Queue = row["queue"].(string)
	job.JobSet = row["job_set"].(string)
	job.PriorityClass = row["priority_class"].(string)
	job.UsefulUS = row["useful_work_us"].(int64)
	job.StartupUS, _ = row["startup_us"].(int64)
	method, _ := row["work_estimation_method"].(string)
	job.WorkEstimationMethod = method
	if checkpoint, ok := row["checkpoint_interval_us"].(int64); ok {
		if checkpoint <= 0 {
			return job, errors.New("checkpoint interval must be null or positive")
		}
		job.CheckpointUS = checkpoint
	}
	priority := row["queue_priority"].(int64)
	if len(job.ID) > maxJobIDBytes || priority < 0 || priority > math.MaxUint32 {
		return job, errors.New("logical job ID or queue priority is outside the supported range")
	}
	job.QueuePriority = uint32(priority)
	if row["gang_id"] != nil || row["gang_cardinality"] != nil || row["gang_node_uniformity_label"] != nil {
		return job, errors.New("the initial replay profile does not support gang jobs")
	}
	if err := job.validate(); err != nil {
		return job, err
	}
	spec := row["requirements_json"].(string)
	if len(spec) > maxRequirementsBytes {
		return job, fmt.Errorf("requirements exceed %d bytes", maxRequirementsBytes)
	}
	var req requirements
	if err := strictJSON([]byte(spec), &req); err != nil {
		return job, fmt.Errorf("requirements_json: %w", err)
	}
	if req.Resources.Requests == nil {
		return job, errors.New("resourceRequirements.requests is required")
	}
	job.Requests = make(v1.ResourceList, len(req.Resources.Requests))
	for name, value := range req.Resources.Requests {
		quantity, err := exactQuantity(value)
		if err != nil {
			return job, fmt.Errorf("resource %q: %w", name, err)
		}
		job.Requests[v1.ResourceName(name)] = quantity
	}
	job.NodeSelector, job.Tolerations = req.NodeSelector, req.Tolerations
	if req.Affinity != nil && req.Affinity.Node != nil {
		job.RequiredNodeAffinity = req.Affinity.Node.Required
	}
	return job, nil
}

func readBounded(r io.Reader, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err == nil && int64(len(raw)) > limit {
		err = fmt.Errorf("input exceeds %d bytes", limit)
	}
	return raw, err
}

func strictJSON(raw []byte, dst any) error {
	if !utf8.Valid(raw) {
		return errors.New("JSON must be UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := checkJSONValue(dec, 0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("extra data after JSON value")
	}
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func checkJSONValue(dec *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting is too deep")
	}
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	var seen map[string]bool
	if delim == '{' {
		seen = make(map[string]bool)
	}
	for count := 0; dec.More(); count++ {
		if count > maxPartitions {
			return errors.New("JSON collection has too many entries")
		}
		if seen != nil {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name := key.(string)
			if seen[name] {
				return fmt.Errorf("duplicate JSON key %q", name)
			}
			seen[name] = true
		}
		if err := checkJSONValue(dec, depth+1); err != nil {
			return err
		}
	}
	_, err = dec.Token()
	return err
}
