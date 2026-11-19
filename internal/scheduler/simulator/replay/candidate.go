package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
)

type runResult struct {
	Version             int       `json:"version"`
	ID                  string    `json:"candidate_id"`
	Status              string    `json:"status"`
	Window              Window    `json:"window"`
	InputSHA256         string    `json:"input_sha256"`
	WorkloadSHA256      string    `json:"workload_sha256"`
	ConfigSHA256        string    `json:"scheduler_config_sha256"`
	FarmSHA256          string    `json:"farm_sha256"`
	BinarySHA256        string    `json:"binary_sha256"`
	AllocatedCPUSeconds *float64  `json:"allocated_cpu_seconds"`
	AvailableCPUSeconds *float64  `json:"available_cpu_seconds"`
	Outcomes            []outcome `json:"outcomes"`
}

type outcome struct {
	ID                     string `json:"logical_job_id"`
	State                  State  `json:"state"`
	Reason                 string `json:"reason,omitempty"`
	Queue                  string `json:"queue"`
	Priority               string `json:"priority_class"`
	SubmittedSeconds       int64  `json:"submitted_at_s"`
	ObservedSeconds        int64  `json:"observed_at_s"`
	FirstAllocationSeconds *int64 `json:"first_allocation_s"`
	FirstUsefulSeconds     *int64 `json:"first_useful_s"`
	Attempts               int64  `json:"attempts"`
	RemainingSeconds       int64  `json:"remaining_s"`
	StartupSeconds         int64  `json:"startup_s"`
	LostSeconds            int64  `json:"lost_s"`
	ProvisionalSeconds     int64  `json:"provisional_s"`
}

func (r *runResult) validate(c candidateRun, inputHash string) error {
	input := c.input
	if r.Version != workerVersion {
		return fmt.Errorf("worker result protocol version %d is incompatible with version %d", r.Version, workerVersion)
	}
	if r.ID != input.ID || r.Status != "complete" || r.Window != input.Window ||
		r.BinarySHA256 != c.binarySHA256 ||
		r.InputSHA256 != inputHash || r.WorkloadSHA256 != input.WorkloadSHA256 ||
		r.FarmSHA256 != digest(input.Farm) || r.ConfigSHA256 != digest(input.Config) {
		return errors.New("incompatible run identity, status or input provenance")
	}
	if r.AllocatedCPUSeconds == nil || r.AvailableCPUSeconds == nil ||
		math.IsNaN(*r.AllocatedCPUSeconds) || math.IsInf(*r.AllocatedCPUSeconds, 0) ||
		math.IsNaN(*r.AvailableCPUSeconds) || math.IsInf(*r.AvailableCPUSeconds, 0) ||
		*r.AllocatedCPUSeconds < 0 || *r.AvailableCPUSeconds < *r.AllocatedCPUSeconds || r.Outcomes == nil {
		return errors.New("invalid CPU accounting or missing outcomes")
	}
	offered := make(map[string]Job, len(input.Jobs))
	for _, job := range input.Jobs {
		if job.SubmittedSeconds >= r.Window.WarmupSeconds && job.SubmittedSeconds < r.Window.EndSeconds {
			offered[job.ID] = job
		}
	}
	for _, row := range r.Outcomes {
		job, ok := offered[row.ID]
		if !ok || row.Queue != job.Queue || row.Priority != job.PriorityClass || row.SubmittedSeconds != job.SubmittedSeconds ||
			!row.valid(r.Window, job.UsefulSeconds) {
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

func (o outcome) valid(window Window, usefulSeconds int64) bool {
	if o.ObservedSeconds < o.SubmittedSeconds || o.Attempts < 0 || o.StartupSeconds < 0 || o.LostSeconds < 0 ||
		o.ProvisionalSeconds < 0 || o.RemainingSeconds < 0 || o.RemainingSeconds > usefulSeconds ||
		(o.FirstAllocationSeconds == nil) != (o.Attempts == 0) {
		return false
	}
	if o.FirstAllocationSeconds != nil && (*o.FirstAllocationSeconds < o.SubmittedSeconds || *o.FirstAllocationSeconds > o.ObservedSeconds ||
		*o.FirstAllocationSeconds >= window.EndSeconds) {
		return false
	}
	if o.FirstUsefulSeconds != nil && (o.FirstAllocationSeconds == nil || *o.FirstUsefulSeconds < *o.FirstAllocationSeconds ||
		*o.FirstUsefulSeconds > o.ObservedSeconds) {
		return false
	}
	switch o.State {
	case Completed:
		return o.ObservedSeconds < window.EndSeconds && o.Attempts > 0 && o.RemainingSeconds == 0
	case Running:
		return o.ObservedSeconds == window.EndSeconds && o.Attempts > 0
	case Queued:
		return o.ObservedSeconds == window.EndSeconds
	default:
		return false
	}
}

// RunComparisonCandidate executes the versioned worker protocol for Compare.
func RunComparisonCandidate(ctx context.Context, inputPath, resultPath string) (resultErr error) {
	raw, err := readLocal(inputPath, maxProtocol)
	if err != nil {
		return err
	}
	var input candidateInput
	if err := strictJSON(raw, &input); err != nil {
		return err
	}
	if input.Version != workerVersion {
		return fmt.Errorf("worker input protocol version %d is incompatible with version %d", input.Version, workerVersion)
	}
	if input.ID != "a" && input.ID != "b" {
		return fmt.Errorf("invalid comparison candidate identity %q", input.ID)
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
	scheduler, err := NewProductionScheduler(ctx, input.Window.WarmupSeconds, cfg, farm, queues)
	if err != nil {
		return err
	}
	dir := filepath.Dir(resultPath)
	attempts, err := createParquet(filepath.Join(dir, attemptsFile), new(attemptRow))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, attempts.close()) }()
	queuesOutput, err := createParquet(filepath.Join(dir, queueIntervalsFile), new(queueIntervalRow))
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, queuesOutput.close()) }()
	observations := comparisonObservations{
		window: input.Window, jobs: make([]outcome, 0, len(input.Jobs)), attempts: attempts, queues: queuesOutput,
	}
	nextIndex := 0
	next := func() (Job, error) {
		if nextIndex == len(input.Jobs) {
			return Job{}, io.EOF
		}
		job := input.Jobs[nextIndex]
		nextIndex++
		if job.SubmittedSeconds < input.Window.WarmupSeconds || job.SubmittedSeconds >= input.Window.EndSeconds {
			_, err := scheduler.makeJob(job)
			return job, err
		}
		return job, nil
	}
	if _, err := Run(ctx, input.Window, next, scheduler, &observations); err != nil {
		return err
	}
	if err := errors.Join(attempts.close(), queuesOutput.close()); err != nil {
		return err
	}
	if err := writeObservationsManifest(ctx, dir); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	hash, err := hashFile(ctx, executable)
	if err != nil {
		return err
	}

	allocated, _ := observations.cpuSeconds.Float64()
	available, _ := cpuSeconds(capacity, input.Window.EndSeconds-input.Window.StartSeconds).Float64()
	return writeJSON(resultPath, runResult{
		Version: workerVersion, ID: input.ID, Status: "complete", Window: input.Window,
		InputSHA256: digest(raw), BinarySHA256: hash, WorkloadSHA256: input.WorkloadSHA256,
		ConfigSHA256: digest(input.Config), FarmSHA256: digest(input.Farm),
		AllocatedCPUSeconds: &allocated, AvailableCPUSeconds: &available, Outcomes: observations.jobs,
	})
}

type comparisonObservations struct {
	window     Window
	jobs       []outcome
	cpuSeconds big.Rat
	attempts   *parquetOutput
	queues     *parquetOutput
}

func (o *comparisonObservations) Job(row JobResult) error {
	o.jobs = append(o.jobs, outcome{
		ID: row.Offered.ID, Queue: row.Offered.Queue, Priority: row.Offered.PriorityClass,
		SubmittedSeconds: row.Offered.SubmittedSeconds, ObservedSeconds: row.ObservedSeconds,
		FirstAllocationSeconds: row.FirstAllocationSeconds, FirstUsefulSeconds: row.FirstUsefulSeconds,
		State: row.State, Reason: row.Reason, Attempts: row.Attempts, RemainingSeconds: row.RemainingSeconds,
		StartupSeconds: row.TotalStartupSeconds, LostSeconds: row.TotalLostSeconds, ProvisionalSeconds: row.ProvisionalSeconds,
	})
	return nil
}

func (o *comparisonObservations) Attempt(row AttemptResult) error {
	start := max(row.AllocatedSeconds, o.window.StartSeconds)
	end := min(row.EndSeconds, o.window.EndSeconds)
	if end > start {
		o.cpuSeconds.Add(&o.cpuSeconds, cpuSeconds(row.Offered.Requests[v1.ResourceCPU], end-start))
	}
	return o.writeAttempt(row)
}

func cpuSeconds(cpu resource.Quantity, durationSeconds int64) *big.Rat {
	// Quantity.AsDec produces a decimal string that big.Rat parses exactly.
	cores, _ := new(big.Rat).SetString(cpu.AsDec().String())
	return cores.Mul(cores, new(big.Rat).SetInt64(durationSeconds))
}
