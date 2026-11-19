package replay

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
)

const maxDurationSeconds = int64(math.MaxInt64) / int64(time.Second)

// Window identifies a simulation by UTC epoch seconds and a cycle length in seconds.
type Window struct {
	WarmupSeconds int64 `json:"warmup_start_s"`
	StartSeconds  int64 `json:"measurement_start_s"`
	EndSeconds    int64 `json:"end_s"`
	CycleSeconds  int64 `json:"cycle_s"`
}

func (w Window) validate() error {
	if w.WarmupSeconds > w.StartSeconds || w.StartSeconds >= w.EndSeconds || uint64(w.EndSeconds)-uint64(w.WarmupSeconds) > uint64(maxDurationSeconds) {
		return errors.New("expected warm-up <= measurement start < end within Go duration range")
	}
	if w.CycleSeconds <= 0 || w.CycleSeconds > maxDurationSeconds {
		return errors.New("cycle length must be positive and within Go duration range")
	}
	return nil
}

// Job describes offered useful work. A zero CheckpointSeconds means no checkpointing.
type Job struct {
	ID                   string            `json:"logical_job_id"`
	SubmittedSeconds     int64             `json:"submitted_at_s"`
	UsefulSeconds        int64             `json:"useful_s"`
	StartupSeconds       int64             `json:"startup_s"`
	CheckpointSeconds    int64             `json:"checkpoint_s"`
	Queue                string            `json:"queue"`
	JobSet               string            `json:"job_set"`
	QueuePriority        uint32            `json:"queue_priority"`
	PriorityClass        string            `json:"priority_class"`
	Requests             v1.ResourceList   `json:"requests"`
	NodeSelector         map[string]string `json:"node_selector"`
	RequiredNodeAffinity *v1.NodeSelector  `json:"required_node_affinity"`
	Tolerations          []v1.Toleration   `json:"tolerations"`
}

func (j Job) validate() error {
	if j.ID == "" || j.UsefulSeconds <= 0 || j.StartupSeconds < 0 || j.StartupSeconds > maxDurationSeconds ||
		j.UsefulSeconds > maxDurationSeconds-j.StartupSeconds || j.CheckpointSeconds < 0 || j.CheckpointSeconds > maxDurationSeconds {
		return fmt.Errorf("job %q has an invalid ID, useful duration, startup or checkpoint interval", j.ID)
	}
	return nil
}

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Completed State = "completed"
	Preempted State = "preempted"
)

type Placement struct {
	SchedulingPool string
	JobID          string
	RunID          string
	Pool           string
	Node           string
}

type Preemption struct {
	JobID  string
	Reason string
}

type Decisions struct {
	Preempt  []Preemption
	Allocate []Placement
}

// Scheduler owns placement and its production-facing lifecycle state. All calls at
// one simulated instant receive the same timestamp.
type Scheduler interface {
	// Admit returns a reason when valid offered work has no eligible pool.
	Admit(ctx context.Context, nowSeconds int64, job Job) (string, error)
	Complete(ctx context.Context, nowSeconds int64, jobID string) error
	Schedule(ctx context.Context, nowSeconds int64) (Decisions, error)
}

type JobResult struct {
	Offered                Job
	FirstAllocationSeconds *int64
	FirstUsefulSeconds     *int64
	RetryWaitSeconds       int64
	TotalStartupSeconds    int64
	TotalLostSeconds       int64
	ProvisionalSeconds     int64
	JobID                  string
	State                  State
	Reason                 string
	Attempts               int64
	ObservedSeconds        int64
	DurableSeconds         int64
	RemainingSeconds       int64
}

type AttemptResult struct {
	Reason             string
	Offered            Job
	JobID              string
	Ordinal            int64
	Placement          Placement
	AllocatedSeconds   int64
	EndSeconds         int64
	State              State
	StartupSeconds     int64
	RetainedSeconds    int64
	LostSeconds        int64
	ProvisionalSeconds int64
}

type QueueInterval struct {
	Offered       Job
	QueuedSeconds int64
	EndSeconds    int64
	Censored      bool
}

type Results interface {
	Queue(QueueInterval) error
	Job(JobResult) error
	Attempt(AttemptResult) error
}

type Stats struct {
	PeakQueued           int   `json:"peak_queued"`
	PeakRunning          int   `json:"peak_running"`
	PeakCompletionEvents int   `json:"peak_completion_events"`
	PeakLookahead        int   `json:"peak_future_lookahead"`
	BeforeWarmup         int64 `json:"before_warmup"`
	AtOrAfterEnd         int64 `json:"at_or_after_end"`
	Admitted             int64 `json:"admitted"`
}

type activeJob struct {
	queuedSeconds          int64
	firstAllocationSeconds *int64
	firstUsefulSeconds     *int64
	retryWaitSeconds       int64
	startupSeconds         int64
	lostSeconds            int64
	job                    Job
	reason                 string
	attempt                *attempt
	attempts               int64
	durableSeconds         int64
}

type attempt struct {
	active            *activeJob
	placement         Placement
	allocatedSeconds  int64
	completionSeconds int64
	index             int
}

type completions []*attempt

func (c completions) Len() int { return len(c) }

func (c completions) Less(i, j int) bool {
	a, b := c[i], c[j]
	if a.completionSeconds != b.completionSeconds {
		return a.completionSeconds < b.completionSeconds
	}
	return a.active.job.ID < b.active.job.ID
}

func (c completions) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
	c[i].index, c[j].index = i, j
}

func (c *completions) Push(value any) {
	a := value.(*attempt)
	a.index = len(*c)
	*c = append(*c, a)
}

func (c *completions) Pop() any {
	last := len(*c) - 1
	a := (*c)[last]
	(*c)[last] = nil
	*c = (*c)[:last]
	a.index = -1
	return a
}

type replayRun struct {
	running int
	active  map[string]*activeJob
	ends    completions
	output  Results
}

func (r *replayRun) closeAttempt(j *activeJob, nowSeconds int64, state State, reason string) (int64, error) {
	a := j.attempt
	if a.index >= 0 {
		heap.Remove(&r.ends, a.index)
	}
	startup := min(nowSeconds-a.allocatedSeconds, j.job.StartupSeconds)
	useful := nowSeconds - a.allocatedSeconds - startup
	retained := useful
	if state != Completed {
		retained = 0
		if interval := j.job.CheckpointSeconds; interval > 0 {
			retained = useful / interval * interval
			// The cutoff observes the state immediately before events at end.
			if state == Running && useful > 0 && retained == useful {
				retained -= interval
			}
		}
	}
	row := AttemptResult{
		Offered: j.job, JobID: j.job.ID, Ordinal: j.attempts, Placement: a.placement, AllocatedSeconds: a.allocatedSeconds,
		EndSeconds: nowSeconds, State: state, Reason: reason, StartupSeconds: startup, RetainedSeconds: retained,
	}
	if state == Preempted {
		row.LostSeconds = useful - retained
	}
	if state == Running {
		row.ProvisionalSeconds = useful - retained
	}
	if useful > 0 && j.firstUsefulSeconds == nil {
		usefulStart := a.allocatedSeconds + startup
		j.firstUsefulSeconds = &usefulStart
	}
	j.startupSeconds += startup
	j.lostSeconds += row.LostSeconds
	j.durableSeconds += retained
	j.queuedSeconds = nowSeconds
	j.attempt = nil
	r.running--
	return row.ProvisionalSeconds, r.output.Attempt(row)
}

func (r *replayRun) observe(j *activeJob, nowSeconds int64, state State, provisionalSeconds int64) error {
	return r.output.Job(JobResult{
		Offered: j.job, FirstAllocationSeconds: j.firstAllocationSeconds, FirstUsefulSeconds: j.firstUsefulSeconds,
		RetryWaitSeconds: j.retryWaitSeconds, TotalStartupSeconds: j.startupSeconds, TotalLostSeconds: j.lostSeconds, ProvisionalSeconds: provisionalSeconds,
		JobID: j.job.ID, State: state, Reason: j.reason, Attempts: j.attempts,
		ObservedSeconds: nowSeconds, DurableSeconds: j.durableSeconds,
		RemainingSeconds: j.job.UsefulSeconds - j.durableSeconds - provisionalSeconds,
	})
}

func (r *replayRun) finish(ctx context.Context, scheduler Scheduler, nowSeconds int64) error {
	for len(r.ends) > 0 && r.ends[0].completionSeconds == nowSeconds {
		j := r.ends[0].active
		if err := scheduler.Complete(ctx, nowSeconds, j.job.ID); err != nil {
			return err
		}
		if _, err := r.closeAttempt(j, nowSeconds, Completed, ""); err != nil {
			return err
		}
		if err := r.observe(j, nowSeconds, Completed, 0); err != nil {
			return err
		}
		delete(r.active, j.job.ID)
	}
	return nil
}

func (r *replayRun) apply(nowSeconds, endSeconds int64, decisions Decisions) error {
	slices.SortFunc(decisions.Preempt, func(a, b Preemption) int { return strings.Compare(a.JobID, b.JobID) })
	for _, preempted := range decisions.Preempt {
		j := r.active[preempted.JobID]
		if j == nil || j.attempt == nil {
			return fmt.Errorf("scheduler preempted job %q without a running attempt", preempted.JobID)
		}
		if _, err := r.closeAttempt(j, nowSeconds, Preempted, preempted.Reason); err != nil {
			return err
		}
	}
	slices.SortFunc(decisions.Allocate, func(a, b Placement) int { return strings.Compare(a.JobID, b.JobID) })
	for _, p := range decisions.Allocate {
		j := r.active[p.JobID]
		if j == nil || j.attempt != nil || j.reason != "" {
			return fmt.Errorf("scheduler allocated job %q without an eligible queued job", p.JobID)
		}
		if err := r.output.Queue(QueueInterval{Offered: j.job, QueuedSeconds: j.queuedSeconds, EndSeconds: nowSeconds}); err != nil {
			return err
		}
		if j.attempts == 0 {
			j.firstAllocationSeconds = &nowSeconds
		} else {
			j.retryWaitSeconds += nowSeconds - j.queuedSeconds
		}
		j.attempts++
		a := &attempt{active: j, placement: p, allocatedSeconds: nowSeconds, index: -1}
		j.attempt = a
		r.running++
		duration := j.job.StartupSeconds + j.job.UsefulSeconds - j.durableSeconds
		if duration < endSeconds-nowSeconds {
			a.completionSeconds = nowSeconds + duration
			heap.Push(&r.ends, a)
		}
	}
	return nil
}

func (r *replayRun) cutoff(ctx context.Context, nowSeconds int64) error {
	ids := make([]string, 0, len(r.active))
	for id := range r.active {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		j := r.active[id]
		state, provisional := Queued, int64(0)
		if j.attempt != nil {
			state = Running
			var err error
			provisional, err = r.closeAttempt(j, nowSeconds, state, "")
			if err != nil {
				return err
			}
		}
		if state == Queued {
			if j.attempts > 0 {
				j.retryWaitSeconds += nowSeconds - j.queuedSeconds
			}
			if err := r.output.Queue(QueueInterval{Offered: j.job, QueuedSeconds: j.queuedSeconds, EndSeconds: nowSeconds, Censored: true}); err != nil {
				return err
			}
		}
		if err := r.observe(j, nowSeconds, state, provisional); err != nil {
			return err
		}
	}
	return nil
}

// Run replays ordered, prevalidated unique logical jobs with one row of lookahead.
func Run(ctx context.Context, window Window, next func() (Job, error), scheduler Scheduler, output Results) (Stats, error) {
	var stats Stats
	if err := window.validate(); err != nil {
		return stats, err
	}
	r := replayRun{active: make(map[string]*activeJob), output: output}
	var offered Job
	hasOffered := false
	read := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		incoming, err := next()
		if errors.Is(err, io.EOF) {
			hasOffered = false
			return nil
		}
		if err != nil {
			return err
		}
		if err := incoming.validate(); err != nil {
			return err
		}
		if hasOffered && (incoming.SubmittedSeconds < offered.SubmittedSeconds ||
			(incoming.SubmittedSeconds == offered.SubmittedSeconds && incoming.ID <= offered.ID)) {
			return fmt.Errorf("job %q arrived out of input order", incoming.ID)
		}
		offered, hasOffered = incoming, true
		stats.PeakLookahead = 1
		return nil
	}
	if err := read(); err != nil {
		return stats, err
	}
	for hasOffered && offered.SubmittedSeconds < window.WarmupSeconds {
		stats.BeforeWarmup++
		if err := read(); err != nil {
			return stats, err
		}
	}
	nextCycle := window.WarmupSeconds
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		now := nextCycle
		if hasOffered {
			now = min(now, offered.SubmittedSeconds)
		}
		if len(r.ends) > 0 {
			now = min(now, r.ends[0].completionSeconds)
		}
		if now >= window.EndSeconds {
			break
		}
		if err := r.finish(ctx, scheduler, now); err != nil {
			return stats, fmt.Errorf("complete at %d: %w", now, err)
		}
		for hasOffered && offered.SubmittedSeconds == now {
			if _, exists := r.active[offered.ID]; exists {
				return stats, fmt.Errorf("duplicate active job %q", offered.ID)
			}
			reason, err := scheduler.Admit(ctx, now, offered)
			if err != nil {
				return stats, fmt.Errorf("admit job %q: %w", offered.ID, err)
			}
			r.active[offered.ID] = &activeJob{job: offered, reason: reason, queuedSeconds: now}
			stats.Admitted++
			if err := read(); err != nil {
				return stats, err
			}
		}
		stats.PeakQueued = max(stats.PeakQueued, len(r.active)-r.running)
		if now == nextCycle {
			decisions, err := scheduler.Schedule(ctx, now)
			if err != nil {
				return stats, fmt.Errorf("schedule at %d: %w", now, err)
			}
			if err := r.apply(now, window.EndSeconds, decisions); err != nil {
				return stats, fmt.Errorf("apply schedule at %d: %w", now, err)
			}
			stats.PeakQueued = max(stats.PeakQueued, len(r.active)-r.running)
			stats.PeakRunning = max(stats.PeakRunning, r.running)
			stats.PeakCompletionEvents = max(stats.PeakCompletionEvents, len(r.ends))
			nextCycle += min(window.CycleSeconds, window.EndSeconds-now)
		}
	}
	for hasOffered {
		stats.AtOrAfterEnd++
		if err := read(); err != nil {
			return stats, err
		}
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	return stats, r.cutoff(ctx, window.EndSeconds)
}
