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
	experimentVersion = 2
	workloadVersion   = 2
	workerVersion     = 3
	maxInput          = 4 << 20
	maxProtocol       = 32 << 20
	maxJobs           = 10_000
	workerCommand     = "compare-candidate"
	orderingPath      = "jobdb.SchedulingOrderCompare/queue-priority"
)

type experiment struct {
	Version                    int         `json:"version"`
	Workload                   string      `json:"workload"`
	Window                     Window      `json:"window"`
	AcknowledgeUnboundedRounds bool        `json:"acknowledge_unbounded_rounds"`
	Change                     string      `json:"change"`
	Code                       *codeChange `json:"code,omitempty"`
	A                          candidate   `json:"a"`
	B                          candidate   `json:"b"`
}

type codeChange struct {
	Path     string `json:"path"`
	Behavior string `json:"behavior"`
}

type candidate struct {
	Binary string      `json:"binary"`
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

type candidateRun struct {
	binary       string
	binarySHA256 string
	input        candidateInput
}

// Compare runs both local simulator binaries with the same offered workload and
// prints the same metrics written to comparison.json. The output must be new.
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
		return errors.New("comparison requires version 2, acknowledge_unbounded_rounds=true and a positive timeout")
	}
	if err := validateWindow(spec.Window); err != nil {
		return err
	}
	base := filepath.Dir(manifest)
	input, err := loadWorkload(resolve(base, spec.Workload), spec.Window)
	if err != nil {
		return err
	}
	a, err := prepareCandidate(ctx, base, "a", spec.A, input)
	if err != nil {
		return fmt.Errorf("candidate A: %w", err)
	}
	b, err := prepareCandidate(ctx, base, "b", spec.B, input)
	if err != nil {
		return fmt.Errorf("candidate B: %w", err)
	}
	if err := validateChange(spec.Change, spec.Code, a, b); err != nil {
		return err
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
	left, err := a.execute(ctx, output, timeout)
	if err != nil {
		return err
	}
	right, err := b.execute(ctx, output, timeout)
	if err != nil {
		return err
	}
	if spec.Change == "none" && (!reflect.DeepEqual(left.Outcomes, right.Outcomes) ||
		*left.AllocatedCPUSeconds != *right.AllocatedCPUSeconds || *left.AvailableCPUSeconds != *right.AvailableCPUSeconds) {
		return errors.New("identical candidates produced different logical outcomes or CPU accounting")
	}
	report := compareResults(spec.Change, spec.Code, left, right)
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
		return candidateInput{}, fmt.Errorf("workload requires version 2, a work estimation method and an explicit list of at most %d jobs", maxJobs)
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

func prepareCandidate(ctx context.Context, base, id string, spec candidate, input candidateInput) (candidateRun, error) {
	if spec.Binary == "" || spec.Farm == "" || spec.Config == "" || len(spec.Queues) == 0 {
		return candidateRun{}, errors.New("binary, farm, scheduler_config and queues are required")
	}
	binary, err := filepath.Abs(resolve(base, spec.Binary))
	if err != nil {
		return candidateRun{}, err
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return candidateRun{}, err
	}
	hash, err := hashFile(ctx, binary)
	if err != nil {
		return candidateRun{}, err
	}
	input.ID, input.Queues = id, slices.Clone(spec.Queues)
	slices.SortFunc(input.Queues, func(a, b queueSpec) int { return strings.Compare(a.Name, b.Name) })
	if input.Farm, err = readLocal(resolve(base, spec.Farm), maxInput); err != nil {
		return candidateRun{}, fmt.Errorf("farm: %w", err)
	}
	if input.Config, err = readLocal(resolve(base, spec.Config), maxInput); err != nil {
		return candidateRun{}, fmt.Errorf("scheduler config: %w", err)
	}
	return candidateRun{binary: binary, binarySHA256: hash, input: input}, nil
}

func validateChange(change string, code *codeChange, a, b candidateRun) error {
	farmChanged := !bytes.Equal(a.input.Farm, b.input.Farm)
	binaryChanged := a.binarySHA256 != b.binarySHA256
	queuesChanged := !reflect.DeepEqual(a.input.Queues, b.input.Queues)
	if !bytes.Equal(a.input.Config, b.input.Config) {
		return errors.New("scheduler_config differs: initial comparisons require identical config files")
	}
	if change != "binary" && code != nil {
		return errors.New("code declaration requires the binary change")
	}
	switch change {
	case "none":
		if !farmChanged && !binaryChanged && !queuesChanged {
			return nil
		}
	case "capacity":
		if farmChanged && !binaryChanged && !queuesChanged {
			return nil
		}
	case "queue_cordon":
		if !farmChanged && !binaryChanged && queuesChanged && sameQueuesWithoutCordons(a.input.Queues, b.input.Queues) {
			return nil
		}
	case "binary":
		if code == nil || code.Path != orderingPath || strings.TrimSpace(code.Behavior) == "" {
			return fmt.Errorf("binary comparison requires a behavior declaration on supported path %q", orderingPath)
		}
		if !farmChanged && binaryChanged && !queuesChanged {
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

func (c candidateRun) execute(parent context.Context, output string, timeout time.Duration) (*runResult, error) {
	dir := filepath.Join(output, c.input.ID)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(c.input)
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
	cmd := exec.CommandContext(ctx, c.binary, workerCommand, inputPath, resultPath)
	cmd.Stdout, cmd.Stderr = log, log
	runErr := cmd.Run()
	if err := errors.Join(runErr, ctx.Err(), log.Close()); err != nil {
		return nil, fmt.Errorf("candidate %s failed, see %s: %w", c.input.ID, logPath, err)
	}
	resultRaw, err := readLocal(resultPath, maxProtocol)
	if err != nil {
		return nil, fmt.Errorf("candidate %s result, see %s: %w", c.input.ID, logPath, err)
	}
	var result runResult
	if err := strictJSON(resultRaw, &result); err != nil {
		return nil, fmt.Errorf("candidate %s result: %w", c.input.ID, err)
	}
	if err := result.validate(c, digest(raw)); err != nil {
		return nil, fmt.Errorf("candidate %s result: %w", c.input.ID, err)
	}
	if err := verifyObservations(ctx, dir); err != nil {
		return nil, fmt.Errorf("candidate %s observations: %w", c.input.ID, err)
	}
	return &result, nil
}

func digest(raw []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func hashFile(ctx context.Context, path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("expected a regular file at %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, contextReader{ctx, f}); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
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
