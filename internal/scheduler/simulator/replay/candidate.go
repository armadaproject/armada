package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
)

type runResult struct {
	Version              int       `json:"version"`
	ID                   string    `json:"candidate_id"`
	Status               string    `json:"status"`
	Profile              string    `json:"profile"`
	Model                string    `json:"model"`
	Window               Window    `json:"window"`
	InputSHA256          string    `json:"input_sha256"`
	WorkloadSHA256       string    `json:"workload_sha256"`
	WorkEstimationMethod string    `json:"work_estimation_method"`
	ConfigSHA256         string    `json:"scheduler_config_sha256"`
	FarmSHA256           string    `json:"farm_sha256"`
	BinarySHA256         string    `json:"binary_sha256"`
	AllocatedCPUSeconds  *float64  `json:"allocated_cpu_seconds"`
	AvailableCPUSeconds  *float64  `json:"available_cpu_seconds"`
	Outcomes             []outcome `json:"outcomes"`
}

type outcome struct {
	ID                string `json:"logical_job_id"`
	State             State  `json:"state"`
	Reason            string `json:"reason,omitempty"`
	Queue             string `json:"queue"`
	Priority          string `json:"priority_class"`
	SubmittedUS       int64  `json:"submitted_at_us"`
	ObservedUS        int64  `json:"observed_at_us"`
	FirstAllocationUS *int64 `json:"first_allocation_us"`
	FirstUsefulUS     *int64 `json:"first_useful_us"`
	Attempts          int64  `json:"attempts"`
	RemainingUS       int64  `json:"remaining_us"`
	StartupUS         int64  `json:"startup_us"`
	LostUS            int64  `json:"lost_us"`
	ProvisionalUS     int64  `json:"provisional_us"`
}

func (r *runResult) validate(c candidateRun, inputHash string) error {
	input := c.input
	if r.Version != version || r.ID != input.ID || r.Status != "complete" || r.Profile != profile ||
		r.Model != model || r.Window != input.Window || r.BinarySHA256 != c.binarySHA256 ||
		r.InputSHA256 != inputHash || r.WorkloadSHA256 != input.WorkloadSHA256 ||
		r.WorkEstimationMethod != input.WorkEstimationMethod ||
		r.FarmSHA256 != digest(input.Farm) || r.ConfigSHA256 != digest(input.Config) {
		return errors.New("incompatible run identity, status, replay profile or input provenance")
	}
	if r.AllocatedCPUSeconds == nil || r.AvailableCPUSeconds == nil ||
		math.IsNaN(*r.AllocatedCPUSeconds) || math.IsInf(*r.AllocatedCPUSeconds, 0) ||
		math.IsNaN(*r.AvailableCPUSeconds) || math.IsInf(*r.AvailableCPUSeconds, 0) ||
		*r.AllocatedCPUSeconds < 0 || *r.AvailableCPUSeconds < *r.AllocatedCPUSeconds || r.Outcomes == nil {
		return errors.New("invalid CPU accounting or missing outcomes")
	}
	offered := make(map[string]Job, len(input.Jobs))
	for _, job := range input.Jobs {
		if job.SubmittedUS >= r.Window.WarmupUS && job.SubmittedUS < r.Window.EndUS {
			offered[job.ID] = job
		}
	}
	for _, row := range r.Outcomes {
		job, ok := offered[row.ID]
		if !ok || row.Queue != job.Queue || row.Priority != job.PriorityClass || row.SubmittedUS != job.SubmittedUS ||
			!row.valid(r.Window, job.UsefulUS) {
			return fmt.Errorf("invalid, duplicate or unexpected outcome for %q", row.ID)
		}
		delete(offered, row.ID)
	}
	if len(offered) != 0 {
		return fmt.Errorf("missing %d logical outcomes", len(offered))
	}
	slices.SortFunc(r.Outcomes, func(a, b outcome) int { return strings.Compare(a.ID, b.ID) })
	return nil
}

func (o outcome) valid(window Window, usefulUS int64) bool {
	if o.ObservedUS < o.SubmittedUS || o.Attempts < 0 || o.StartupUS < 0 || o.LostUS < 0 ||
		o.ProvisionalUS < 0 || o.RemainingUS < 0 || o.RemainingUS > usefulUS ||
		(o.FirstAllocationUS == nil) != (o.Attempts == 0) {
		return false
	}
	if o.FirstAllocationUS != nil && (*o.FirstAllocationUS < o.SubmittedUS || *o.FirstAllocationUS > o.ObservedUS ||
		*o.FirstAllocationUS >= window.EndUS) {
		return false
	}
	if o.FirstUsefulUS != nil && (o.FirstAllocationUS == nil || *o.FirstUsefulUS < *o.FirstAllocationUS ||
		*o.FirstUsefulUS > o.ObservedUS) {
		return false
	}
	switch o.State {
	case Completed:
		return o.ObservedUS < window.EndUS && o.Attempts > 0 && o.RemainingUS == 0
	case Running:
		return o.ObservedUS == window.EndUS && o.Attempts > 0
	case Queued:
		return o.ObservedUS == window.EndUS
	default:
		return false
	}
}

// RunComparisonCandidate executes the versioned worker protocol for Compare.
func RunComparisonCandidate(ctx context.Context, inputPath, resultPath string) error {
	raw, err := readLocal(inputPath, maxProtocol)
	if err != nil {
		return err
	}
	var input candidateInput
	if err := strictJSON(raw, &input); err != nil {
		return err
	}
	if input.Version != version || (input.ID != "a" && input.ID != "b") {
		return errors.New("invalid comparison candidate protocol or identity")
	}
	if err := validateWindow(input.Window); err != nil {
		return err
	}
	cfg, err := readSchedulerConfig(input.Config)
	if err != nil {
		return fmt.Errorf("scheduler config: %w", err)
	}
	farm, capacity, err := readFarm(input.Farm, cfg)
	if err != nil {
		return fmt.Errorf("farm: %w", err)
	}
	queues := make([]*api.Queue, 0, len(input.Queues))
	for _, queue := range input.Queues {
		queues = append(queues, &api.Queue{
			Name: queue.Name, PriorityFactor: 1 / queue.Weight, Cordoned: queue.Cordoned,
			ResourceLimitsByPriorityClassName: queue.Limits,
		})
	}
	scheduler, err := NewProductionScheduler(ctx, input.Window.WarmupUS, cfg, farm, queues)
	if err != nil {
		return err
	}
	observations := comparisonObservations{window: input.Window, jobs: make([]outcome, 0, len(input.Jobs))}
	nextIndex := 0
	next := func() (Job, error) {
		if nextIndex == len(input.Jobs) {
			return Job{}, io.EOF
		}
		job := input.Jobs[nextIndex]
		nextIndex++
		if job.SubmittedUS < input.Window.WarmupUS || job.SubmittedUS >= input.Window.EndUS {
			_, err := scheduler.makeJob(job)
			return job, err
		}
		return job, nil
	}
	if _, err := Run(ctx, input.Window, next, scheduler, &observations); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	hash, err := hashExecutable(ctx, executable)
	if err != nil {
		return err
	}

	allocated, _ := observations.cpuSeconds.Float64()
	available, _ := cpuSeconds(capacity, input.Window.EndUS-input.Window.StartUS).Float64()
	return writeJSON(resultPath, runResult{
		Version: version, ID: input.ID, Status: "complete", Profile: profile, Model: model, Window: input.Window,
		InputSHA256: digest(raw), BinarySHA256: hash, WorkloadSHA256: input.WorkloadSHA256,
		WorkEstimationMethod: input.WorkEstimationMethod, ConfigSHA256: digest(input.Config), FarmSHA256: digest(input.Farm),
		AllocatedCPUSeconds: &allocated, AvailableCPUSeconds: &available, Outcomes: observations.jobs,
	})
}

type comparisonObservations struct {
	window     Window
	jobs       []outcome
	cpuSeconds big.Rat
}

func (o *comparisonObservations) Queue(QueueInterval) error { return nil }

func (o *comparisonObservations) Job(row JobResult) error {
	o.jobs = append(o.jobs, outcome{
		ID: row.Offered.ID, Queue: row.Offered.Queue, Priority: row.Offered.PriorityClass,
		SubmittedUS: row.Offered.SubmittedUS, ObservedUS: row.ObservedUS,
		FirstAllocationUS: row.FirstAllocationUS, FirstUsefulUS: row.FirstUsefulUS,
		State: row.State, Reason: row.Reason, Attempts: row.Attempts, RemainingUS: row.RemainingUS,
		StartupUS: row.TotalStartupUS, LostUS: row.TotalLostUS, ProvisionalUS: row.ProvisionalUS,
	})
	return nil
}

func (o *comparisonObservations) Attempt(row AttemptResult) error {
	start := max(row.AllocatedUS, o.window.StartUS)
	end := min(row.EndUS, o.window.EndUS)
	if end > start {
		o.cpuSeconds.Add(&o.cpuSeconds, cpuSeconds(row.Offered.Requests[v1.ResourceCPU], end-start))
	}
	return nil
}

func cpuSeconds(cpu resource.Quantity, durationUS int64) *big.Rat {
	// Quantity.AsDec produces a decimal string that big.Rat parses exactly.
	cores, _ := new(big.Rat).SetString(cpu.AsDec().String())
	return cores.Mul(cores, big.NewRat(durationUS, 1_000_000))
}
