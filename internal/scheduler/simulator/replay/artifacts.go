package replay

import (
	"context"
	"encoding/json"
	"errors"
	"math/bits"
	"os"
	"path/filepath"

	parquetwriter "github.com/xitongsys/parquet-go/writer"

	v1 "k8s.io/api/core/v1"
)

const (
	logicalFile = "logical_jobs.parquet"
	attemptFile = "attempts.parquet"
	seriesFile  = "series.parquet"
	summaryFile = "summary.json"
)

type logicalRow struct {
	JobID            string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Queue            string `parquet:"name=queue, type=BYTE_ARRAY, convertedtype=UTF8"`
	JobSet           string `parquet:"name=job_set, type=BYTE_ARRAY, convertedtype=UTF8"`
	QueuePriority    int64  `parquet:"name=queue_priority, type=INT64"`
	Priority         string `parquet:"name=priority_class, type=BYTE_ARRAY, convertedtype=UTF8"`
	Requests         string `parquet:"name=requests_json, type=BYTE_ARRAY, convertedtype=UTF8"`
	SubmittedUS      int64  `parquet:"name=submitted_at_us, type=INT64"`
	OfferedUS        int64  `parquet:"name=useful_work_us, type=INT64"`
	StartupPolicyUS  int64  `parquet:"name=startup_policy_us, type=INT64"`
	CheckpointUS     *int64 `parquet:"name=checkpoint_interval_us, type=INT64"`
	EstimationMethod string `parquet:"name=work_estimation_method, type=BYTE_ARRAY, convertedtype=UTF8"`
	FirstLeaseUS     *int64 `parquet:"name=first_allocation_at_us, type=INT64"`
	FirstUsefulUS    *int64 `parquet:"name=first_useful_start_at_us, type=INT64"`
	CompletionUS     *int64 `parquet:"name=completed_at_us, type=INT64"`
	ObservedUS       int64  `parquet:"name=observed_at_us, type=INT64"`
	FinalState       string `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	Reason           string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	Cohort           string `parquet:"name=cohort, type=BYTE_ARRAY, convertedtype=UTF8"`
	Censored         bool   `parquet:"name=censored, type=BOOLEAN"`
	Attempts         int64  `parquet:"name=attempts, type=INT64"`
	RetryWaitUS      int64  `parquet:"name=retry_wait_us, type=INT64"`
	StartupUS        int64  `parquet:"name=occupied_startup_us, type=INT64"`
	DurableUS        int64  `parquet:"name=durable_us, type=INT64"`
	LostUS           int64  `parquet:"name=lost_us, type=INT64"`
	ProvisionalUS    int64  `parquet:"name=provisional_us, type=INT64"`
	RemainingUS      int64  `parquet:"name=remaining_us, type=INT64"`
}

type attemptRow struct {
	Reason         string `parquet:"name=reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	SchedulingPool string `parquet:"name=scheduling_pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	JobID          string `parquet:"name=logical_job_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Ordinal        int64  `parquet:"name=attempt, type=INT64"`
	RunID          string `parquet:"name=run_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Pool           string `parquet:"name=pool, type=BYTE_ARRAY, convertedtype=UTF8"`
	Node           string `parquet:"name=node, type=BYTE_ARRAY, convertedtype=UTF8"`
	Requests       string `parquet:"name=requests_json, type=BYTE_ARRAY, convertedtype=UTF8"`
	AllocatedUS    int64  `parquet:"name=allocated_at_us, type=INT64"`
	UsefulStartUS  *int64 `parquet:"name=useful_start_at_us, type=INT64"`
	EndUS          int64  `parquet:"name=end_at_us, type=INT64"`
	State          string `parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	StartupUS      int64  `parquet:"name=startup_us, type=INT64"`
	RetainedUS     int64  `parquet:"name=retained_us, type=INT64"`
	LostUS         int64  `parquet:"name=lost_us, type=INT64"`
	ProvisionalUS  int64  `parquet:"name=provisional_us, type=INT64"`
}

type parquetOutput struct {
	file *os.File
	w    *parquetwriter.ParquetWriter
}

func createParquet(path string, row any) (*parquetOutput, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	w, err := parquetwriter.NewParquetWriterFromWriter(f, row, 1)
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	w.RowGroupSize = 4 << 20
	return &parquetOutput{file: f, w: w}, nil
}

func (p *parquetOutput) close() error {
	if p == nil || p.file == nil {
		return nil
	}
	file := p.file
	p.file = nil
	return errors.Join(p.w.WriteStop(), file.Sync(), file.Close())
}

type artifactOutput struct {
	dir      string
	window   Window
	method   string
	logical  *parquetOutput
	attempts *parquetOutput
	spool    *seriesSpool
	summary  runSummary
}

type groupCounts struct {
	Arrived      int64     `json:"arrived"`
	CarryIn      int64     `json:"carry_in"`
	Completed    int64     `json:"completed"`
	QueuedAtEnd  int64     `json:"queued_at_end"`
	RunningAtEnd int64     `json:"running_at_end"`
	Ineligible   int64     `json:"ineligible_at_end"`
	FirstLeaseN  int64     `json:"first_allocation_observations"`
	FirstLeaseUS [65]int64 `json:"first_allocation_delay_us_log2_histogram"`
	Preemptions  int64     `json:"preemptions"`
}

type runSummary struct {
	Version  int                                `json:"version"`
	Window   Window                             `json:"window"`
	Input    Stats                              `json:"input_counts"`
	Capacity map[string]v1.ResourceList         `json:"allocatable_by_pool"`
	Counts   map[string]map[string]*groupCounts `json:"counts_by_queue_and_priority"`
	Work     []seriesRow                        `json:"window_resource_time"`
}

func openArtifacts(dir string, window Window, method string, spool *seriesSpool) (*artifactOutput, error) {
	a := &artifactOutput{
		dir: dir, window: window, method: method, spool: spool,
		summary: runSummary{Version: 1, Window: window, Counts: make(map[string]map[string]*groupCounts)},
	}
	var err error
	a.logical, err = createParquet(filepath.Join(dir, logicalFile), new(logicalRow))
	if err != nil {
		return nil, err
	}
	a.attempts, err = createParquet(filepath.Join(dir, attemptFile), new(attemptRow))
	if err != nil {
		return nil, errors.Join(err, a.logical.close())
	}
	return a, nil
}

func (a *artifactOutput) counts(job Job) *groupCounts {
	byPriority := a.summary.Counts[job.Queue]
	if byPriority == nil {
		byPriority = make(map[string]*groupCounts)
		a.summary.Counts[job.Queue] = byPriority
	}
	counts := byPriority[job.PriorityClass]
	if counts == nil {
		counts = new(groupCounts)
		byPriority[job.PriorityClass] = counts
	}
	return counts
}

func (a *artifactOutput) Job(result JobResult) error {
	job := result.Offered
	raw, err := json.Marshal(job.Requests)
	if err != nil {
		return err
	}
	cohort := "arrival"
	counts := a.counts(job)
	if job.SubmittedUS < a.window.StartUS {
		cohort = "carry_in"
		if result.ObservedUS >= a.window.StartUS {
			counts.CarryIn++
		} else {
			cohort = "warmup_only"
		}
	} else {
		counts.Arrived++
		if result.FirstAllocationUS != nil {
			delay := *result.FirstAllocationUS - job.SubmittedUS
			counts.FirstLeaseN++
			counts.FirstLeaseUS[bits.Len64(uint64(delay))]++
		}
	}
	method := job.WorkEstimationMethod
	if method == "" {
		method = a.method
	}
	row := logicalRow{
		JobID: job.ID, Queue: job.Queue, JobSet: job.JobSet, QueuePriority: int64(job.QueuePriority),
		Priority: job.PriorityClass, Requests: string(raw), SubmittedUS: job.SubmittedUS,
		OfferedUS: job.UsefulUS, StartupPolicyUS: job.StartupUS, EstimationMethod: method,
		FirstLeaseUS: result.FirstAllocationUS, FirstUsefulUS: result.FirstUsefulUS,
		ObservedUS: result.ObservedUS, FinalState: string(result.State), Reason: result.Reason,
		Cohort: cohort, Censored: result.State != Completed, Attempts: result.Attempts,
		RetryWaitUS: result.RetryWaitUS, StartupUS: result.TotalStartupUS, DurableUS: result.DurableUS,
		LostUS: result.TotalLostUS, ProvisionalUS: result.ProvisionalUS, RemainingUS: result.RemainingUS,
	}
	if job.CheckpointUS > 0 {
		row.CheckpointUS = &job.CheckpointUS
	}
	if cohort == "arrival" {
		if err := a.spool.write(seriesRecord{Kind: arrivalEvent, StartUS: job.SubmittedUS, Queue: job.Queue, Priority: job.PriorityClass}); err != nil {
			return err
		}
	}
	switch result.State {
	case Completed:
		row.CompletionUS = &result.ObservedUS
		if result.ObservedUS >= a.window.StartUS {
			counts.Completed++
			if err := a.spool.write(seriesRecord{Kind: completionEvent, StartUS: result.ObservedUS, Queue: job.Queue, Priority: job.PriorityClass}); err != nil {
				return err
			}
		}
	case Queued:
		counts.QueuedAtEnd++
		if result.Reason != "" {
			counts.Ineligible++
		}
	case Running:
		counts.RunningAtEnd++
	}
	return a.logical.w.Write(row)
}

func (a *artifactOutput) Attempt(result AttemptResult) error {
	job := result.Offered
	raw, err := json.Marshal(job.Requests)
	if err != nil {
		return err
	}
	row := attemptRow{
		JobID: result.JobID, Ordinal: result.Ordinal, RunID: result.Placement.RunID,
		Pool: result.Placement.Pool, SchedulingPool: result.Placement.SchedulingPool, Node: result.Placement.Node, Requests: string(raw),
		AllocatedUS: result.AllocatedUS, EndUS: result.EndUS, State: string(result.State), Reason: result.Reason,
		StartupUS: result.StartupUS, RetainedUS: result.RetainedUS,
		LostUS: result.LostUS, ProvisionalUS: result.ProvisionalUS,
	}
	if result.RetainedUS+result.LostUS+result.ProvisionalUS > 0 {
		start := result.AllocatedUS + result.StartupUS
		row.UsefulStartUS = &start
	}
	if result.State == Preempted && result.EndUS >= a.window.StartUS {
		a.counts(job).Preemptions++
		if err := a.spool.write(seriesRecord{Kind: preemptionEvent, StartUS: result.EndUS, Queue: job.Queue, Priority: job.PriorityClass}); err != nil {
			return err
		}
	}
	if result.EndUS >= a.window.StartUS {
		if err := a.spool.write(seriesRecord{
			Kind: executionInterval, Queue: job.Queue, Priority: job.PriorityClass, Pool: row.Pool,
			StartUS: row.AllocatedUS, EndUS: row.EndUS, Censored: result.State == Running,
			StartupUS: row.StartupUS, RetainedUS: row.RetainedUS, LostUS: row.LostUS, Requests: job.Requests,
		}); err != nil {
			return err
		}
	}
	return a.attempts.w.Write(row)
}

func (a *artifactOutput) Queue(interval QueueInterval) error {
	if interval.EndUS <= a.window.StartUS || (interval.EndUS <= interval.QueuedUS && !interval.Censored) {
		return nil
	}
	return a.spool.write(seriesRecord{
		Kind: queueInterval, Queue: interval.Offered.Queue, Priority: interval.Offered.PriorityClass,
		StartUS: interval.QueuedUS, EndUS: interval.EndUS, Censored: interval.Censored,
	})
}

func (a *artifactOutput) finish(ctx context.Context, bucketUS int64, stats Stats, capacity map[string]v1.ResourceList) error {
	if err := errors.Join(a.logical.close(), a.attempts.close(), a.spool.flush()); err != nil {
		return err
	}
	work, err := a.spool.aggregate(ctx, filepath.Join(a.dir, seriesFile), a.window, bucketUS)
	if err != nil {
		return err
	}
	a.summary.Work = work
	a.summary.Input = stats
	a.summary.Capacity = capacity
	return writeJSON(filepath.Join(a.dir, summaryFile), a.summary)
}

func writeJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return errors.Join(enc.Encode(value), f.Sync(), f.Close())
}

func writeRunManifest(dir string, value any) error {
	f, err := os.CreateTemp(dir, ".run-*.json")
	if err != nil {
		return err
	}
	path := f.Name()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := errors.Join(enc.Encode(value), f.Sync(), f.Close()); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	if err := os.Rename(path, filepath.Join(dir, "run.json")); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	return nil
}

func hashOutput(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest, readErr := hashBytes(contextReader{ctx, f})
	return digest, errors.Join(readErr, f.Close())
}
