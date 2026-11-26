package replay

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
)

const (
	experimentVersion = 1
	workloadVersion   = 1
	workerVersion     = 1
	maxInput          = 4 << 20
	maxProtocol       = 32 << 20
	maxJobs           = 10_000
	workerCommand     = "compare-candidate"
)

type experiment struct {
	Version                    int    `json:"version"`
	Workload                   string `json:"workload"`
	Window                     Window `json:"window"`
	AcknowledgeUnboundedRounds bool   `json:"acknowledge_unbounded_rounds"`
	Change                     string `json:"change"`

	A candidate `json:"a"`
	B candidate `json:"b"`
}

type candidate struct {
	Farm   string      `json:"farm"`
	Config string      `json:"scheduler_config"`
	Queues []queueSpec `json:"queues"`
}

type workloadSpec struct {
	Version              int           `json:"version"`
	WorkEstimationMethod string        `json:"work_estimation_method"`
	Jobs                 []workloadJob `json:"jobs"`
}

type workloadJob struct {
	Job
	Requests map[string]string `json:"requests"`
}

type candidateInput struct {
	Version              int         `json:"version"`
	ID                   string      `json:"candidate_id"`
	Window               Window      `json:"window"`
	Jobs                 []Job       `json:"jobs"`
	WorkloadSHA256       string      `json:"workload_sha256"`
	WorkEstimationMethod string      `json:"work_estimation_method"`
	Farm                 []byte      `json:"farm_yaml"`
	Config               []byte      `json:"scheduler_config_yaml"`
	Queues               []queueSpec `json:"queues"`
}

// Compare runs both candidates with the same executable and offered workload
// and prints the metrics written to comparison.json. The output must be new.
func Compare(ctx context.Context, manifest, output string, timeout time.Duration, table io.Writer) error {
	raw, err := readLocal(manifest, maxInput)
	if err != nil {
		return err
	}
	var spec experiment
	if err := strictJSON(raw, &spec); err != nil {
		return fmt.Errorf("experiment: %w", err)
	}
	if spec.Version != experimentVersion || !spec.AcknowledgeUnboundedRounds || timeout <= 0 {
		return errors.New("comparison requires version 1, acknowledge_unbounded_rounds=true and a positive timeout")
	}
	if err := validateWindow(spec.Window); err != nil {
		return err
	}
	base := filepath.Dir(manifest)
	input, err := loadWorkload(resolve(base, spec.Workload), spec.Window)
	if err != nil {
		return err
	}
	a, err := prepareCandidate(base, "a", spec.A, input)
	if err != nil {
		return fmt.Errorf("candidate A: %w", err)
	}
	b, err := prepareCandidate(base, "b", spec.B, input)
	if err != nil {
		return fmt.Errorf("candidate B: %w", err)
	}
	if err := validateChange(spec.Change, a, b); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.Open(executable)
	if err != nil {
		return err
	}
	defer binary.Close()
	binaryInfo, err := binary.Stat()
	if err != nil {
		return err
	}
	if !binaryInfo.Mode().IsRegular() {
		return fmt.Errorf("expected a regular simulator executable at %s", executable)
	}
	binaryHash, err := hashExecutable(ctx, binary)
	if err != nil {
		return fmt.Errorf("hash simulator executable: %w", err)
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return err
	}
	if err := os.Mkdir(output, 0o755); err != nil {
		return fmt.Errorf("create new comparison directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(output, "experiment.json"), raw, 0o644); err != nil {
		return err
	}
	left, err := executeCandidate(ctx, executable, output, timeout, a)
	if err != nil {
		return err
	}
	if err := verifyExecutable(executable, binaryInfo); err != nil {
		return err
	}
	right, err := executeCandidate(ctx, executable, output, timeout, b)
	if err != nil {
		return err
	}
	if err := verifyExecutable(executable, binaryInfo); err != nil {
		return err
	}
	if spec.Change == "none" && (!reflect.DeepEqual(left.Outcomes, right.Outcomes) ||
		*left.AllocatedCPUSeconds != *right.AllocatedCPUSeconds || *left.AvailableCPUSeconds != *right.AvailableCPUSeconds) {
		return errors.New("identical candidates produced different logical outcomes or CPU accounting")
	}
	report := compareResults(spec.Change, left, right)
	report.BinarySHA256 = binaryHash
	if err := writeJSON(filepath.Join(output, "comparison.json"), report); err != nil {
		return err
	}
	return report.print(table)
}

func validateWindow(window Window) error {
	if err := window.validate(); err != nil {
		return err
	}
	if window.WarmupSeconds < -maxDurationSeconds || window.EndSeconds > maxDurationSeconds {
		return errors.New("replay window exceeds Armada nanosecond timestamp range")
	}
	return nil
}

func loadWorkload(path string, window Window) (candidateInput, error) {
	raw, err := readLocal(path, maxInput)
	if err != nil {
		return candidateInput{}, fmt.Errorf("workload: %w", err)
	}
	var spec workloadSpec
	if err := strictJSON(raw, &spec); err != nil {
		return candidateInput{}, fmt.Errorf("workload: %w", err)
	}
	if spec.Version != workloadVersion || strings.TrimSpace(spec.WorkEstimationMethod) == "" || spec.Jobs == nil || len(spec.Jobs) > maxJobs {
		return candidateInput{}, fmt.Errorf("workload requires version 1, a work estimation method and an explicit list of at most %d jobs", maxJobs)
	}
	jobs := make([]Job, 0, len(spec.Jobs))
	ids := make(map[string]bool, len(spec.Jobs))
	for _, entry := range spec.Jobs {
		job := entry.Job
		if err := job.validate(); err != nil {
			return candidateInput{}, err
		}
		if ids[job.ID] || len(entry.Requests) == 0 {
			return candidateInput{}, fmt.Errorf("job %q must have a unique ID and resource requests", job.ID)
		}
		ids[job.ID] = true
		job.Requests = make(v1.ResourceList, len(entry.Requests))
		for name, text := range entry.Requests {
			quantity, err := exactQuantity(text)
			if err != nil {
				return candidateInput{}, fmt.Errorf("job %q resource %q: %w", job.ID, name, err)
			}
			job.Requests[v1.ResourceName(name)] = quantity
		}
		jobs = append(jobs, job)
	}
	slices.SortFunc(jobs, func(a, b Job) int {
		if a.SubmittedSeconds != b.SubmittedSeconds {
			return cmp.Compare(a.SubmittedSeconds, b.SubmittedSeconds)
		}
		return strings.Compare(a.ID, b.ID)
	})
	return candidateInput{
		Version: workerVersion, Window: window, Jobs: jobs,
		WorkloadSHA256: digest(raw), WorkEstimationMethod: spec.WorkEstimationMethod,
	}, nil
}

func resolve(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

func prepareCandidate(base, id string, spec candidate, input candidateInput) (candidateInput, error) {
	if spec.Farm == "" || spec.Config == "" || len(spec.Queues) == 0 {
		return candidateInput{}, errors.New("farm, scheduler_config and queues are required")
	}
	farm, err := readLocal(resolve(base, spec.Farm), maxInput)
	if err != nil {
		return candidateInput{}, fmt.Errorf("farm: %w", err)
	}
	cfg, err := readLocal(resolve(base, spec.Config), maxInput)
	if err != nil {
		return candidateInput{}, fmt.Errorf("scheduler config: %w", err)
	}
	input.ID, input.Queues = id, slices.Clone(spec.Queues)
	slices.SortFunc(input.Queues, func(a, b queueSpec) int { return strings.Compare(a.Name, b.Name) })
	input.Farm, input.Config = farm, cfg
	return input, nil
}

func validateChange(change string, a, b candidateInput) error {
	farmChanged := !bytes.Equal(a.Farm, b.Farm)
	queuesChanged := !reflect.DeepEqual(a.Queues, b.Queues)
	if !bytes.Equal(a.Config, b.Config) {
		return errors.New("scheduler_config differs: initial comparisons require identical config files")
	}
	switch change {
	case "none":
		if !farmChanged && !queuesChanged {
			return nil
		}
	case "capacity":
		if farmChanged && !queuesChanged {
			return nil
		}
	case "queue_cordon":
		if !farmChanged && queuesChanged && sameQueuesWithoutCordons(a.Queues, b.Queues) {
			return nil
		}
	default:
		return fmt.Errorf("unknown experiment change %q", change)
	}
	return fmt.Errorf("candidate inputs do not match the declared %q change", change)
}

func sameQueuesWithoutCordons(a, b []queueSpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		left, right := a[i], b[i]
		left.Cordoned, right.Cordoned = false, false
		if !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

func executeCandidate(parent context.Context, executable, output string, timeout time.Duration, input candidateInput) (*runResult, error) {
	dir := filepath.Join(output, input.ID)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	inputPath := filepath.Join(dir, "input.json")
	if err := os.WriteFile(inputPath, raw, 0o644); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "process.log")
	log, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	resultPath := filepath.Join(dir, "run.json")
	cmd := exec.CommandContext(ctx, executable, workerCommand, inputPath, resultPath)
	cmd.Stdout, cmd.Stderr = log, log
	runErr := cmd.Run()
	if err := errors.Join(runErr, ctx.Err(), log.Close()); err != nil {
		return nil, fmt.Errorf("candidate %s failed, see %s: %w", input.ID, logPath, err)
	}
	resultRaw, err := readLocal(resultPath, maxProtocol)
	if err != nil {
		return nil, fmt.Errorf("candidate %s result, see %s: %w", input.ID, logPath, err)
	}
	var result runResult
	if err := strictJSON(resultRaw, &result); err != nil {
		return nil, fmt.Errorf("candidate %s result: %w", input.ID, err)
	}
	if err := result.validate(input, digest(raw)); err != nil {
		return nil, fmt.Errorf("candidate %s result: %w", input.ID, err)
	}
	if err := verifyObservations(dir); err != nil {
		return nil, fmt.Errorf("candidate %s observations: %w", input.ID, err)
	}
	return &result, nil
}

func digest(raw []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func hashExecutable(ctx context.Context, executable *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, contextReader{ctx, executable}); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func verifyExecutable(path string, original os.FileInfo) error {
	current, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("recheck simulator executable: %w", err)
	}
	if !os.SameFile(original, current) || original.Size() != current.Size() ||
		!original.ModTime().Equal(current.ModTime()) {
		return fmt.Errorf("simulator executable changed during comparison: %s", path)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buf []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buf)
}

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(append(raw, '\n'))
	return errors.Join(err, f.Close())
}
