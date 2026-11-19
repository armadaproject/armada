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

const maxDurationUS = int64(math.MaxInt64) / int64(time.Microsecond)

// Window identifies a simulation by UTC epoch microseconds and a cycle length in microseconds.
type Window struct {
	WarmupUS int64 `json:"warmup_start_us"`
	StartUS  int64 `json:"measurement_start_us"`
	EndUS    int64 `json:"end_us"`
	CycleUS  int64 `json:"cycle_us"`
}

func (w Window) validate() error {
	if w.WarmupUS > w.StartUS || w.StartUS >= w.EndUS || uint64(w.EndUS)-uint64(w.WarmupUS) > uint64(maxDurationUS) {
		return errors.New("expected warm-up <= measurement start < end within Go duration range")
	}
	if w.CycleUS <= 0 || w.CycleUS > maxDurationUS {
		return errors.New("cycle length must be positive and within Go duration range")
	}
	return nil
}

// Job describes offered useful work. A zero CheckpointUS means no checkpointing.
type Job struct {
	ID                   string            `json:"logical_job_id"`
	SubmittedUS          int64             `json:"submitted_at_us"`
	UsefulUS             int64             `json:"useful_us"`
	StartupUS            int64             `json:"startup_us"`
	CheckpointUS         int64             `json:"checkpoint_us"`
	Queue                string            `json:"queue"`
	JobSet               string            `json:"job_set"`
	QueuePriority        uint32            `json:"queue_priority"`
	PriorityClass        string            `json:"priority_class"`
	Requests             v1.ResourceList   `json:"requests"`
	NodeSelector         map[string]string `json:"node_selector"`
	RequiredNodeAffinity *v1.NodeSelector  `json:"required_node_affinity"`
	Tolerations          []v1.Toleration   `json:"tolerations"`
	WorkEstimationMethod string            `json:"work_estimation_method"`
}

func (j Job) validate() error {
	if j.ID == "" || j.UsefulUS <= 0 || j.StartupUS < 0 || j.StartupUS > maxDurationUS ||
		j.UsefulUS > maxDurationUS-j.StartupUS || j.CheckpointUS < 0 || j.CheckpointUS > maxDurationUS {
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
	Admit(ctx context.Context, nowUS int64, job Job) (string, error)
	Complete(ctx context.Context, nowUS int64, jobID string) error
	Schedule(ctx context.Context, nowUS int64) (Decisions, error)
}

type JobResult struct {
	Offered           Job
	FirstAllocationUS *int64
	FirstUsefulUS     *int64
	RetryWaitUS       int64
	TotalStartupUS    int64
	TotalLostUS       int64
	ProvisionalUS     int64
	JobID             string
	State             State
	Reason            string
	Attempts          int64
	ObservedUS        int64
	DurableUS         int64
	RemainingUS       int64
}

type AttemptResult struct {
	Reason        string
	Offered       Job
	JobID         string
	Ordinal       int64
	Placement     Placement
	AllocatedUS   int64
	EndUS         int64
	State         State
	StartupUS     int64
	RetainedUS    int64
	LostUS        int64
	ProvisionalUS int64
}

type QueueInterval struct {
	Offered  Job
	QueuedUS int64
	EndUS    int64
	Censored bool
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
	queuedUS          int64
	firstAllocationUS *int64
	firstUsefulUS     *int64
	retryWaitUS       int64
	startupUS         int64
	lostUS            int64
	job               Job
	reason            string
	attempt           *attempt
	attempts          int64
	durableUS         int64
}

type attempt struct {
	active       *activeJob
	placement    Placement
	allocatedUS  int64
	completionUS int64
	index        int
}

type completions []*attempt

func (c completions) Len() int { return len(c) }

func (c completions) Less(i, j int) bool {
	a, b := c[i], c[j]
	if a.completionUS != b.completionUS {
		return a.completionUS < b.completionUS
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

func (r *replayRun) closeAttempt(j *activeJob, nowUS int64, state State, reason string) (int64, error) {
	a := j.attempt
	if a.index >= 0 {
		heap.Remove(&r.ends, a.index)
	}
	startup := min(nowUS-a.allocatedUS, j.job.StartupUS)
	useful := nowUS - a.allocatedUS - startup
	retained := useful
	if state != Completed {
		retained = 0
		if interval := j.job.CheckpointUS; interval > 0 {
			retained = useful / interval * interval
			// The cutoff observes the state immediately before events at end.
			if state == Running && useful > 0 && retained == useful {
				retained -= interval
			}
		}
	}
	row := AttemptResult{
		Offered: j.job, JobID: j.job.ID, Ordinal: j.attempts, Placement: a.placement, AllocatedUS: a.allocatedUS,
		EndUS: nowUS, State: state, Reason: reason, StartupUS: startup, RetainedUS: retained,
	}
	if state == Preempted {
		row.LostUS = useful - retained
	}
	if state == Running {
		row.ProvisionalUS = useful - retained
	}
	if useful > 0 && j.firstUsefulUS == nil {
		usefulStart := a.allocatedUS + startup
		j.firstUsefulUS = &usefulStart
	}
	j.startupUS += startup
	j.lostUS += row.LostUS
	j.durableUS += retained
	j.queuedUS = nowUS
	j.attempt = nil
	r.running--
	return row.ProvisionalUS, r.output.Attempt(row)
}

func (r *replayRun) observe(j *activeJob, nowUS int64, state State, provisionalUS int64) error {
	return r.output.Job(JobResult{
		Offered: j.job, FirstAllocationUS: j.firstAllocationUS, FirstUsefulUS: j.firstUsefulUS,
		RetryWaitUS: j.retryWaitUS, TotalStartupUS: j.startupUS, TotalLostUS: j.lostUS, ProvisionalUS: provisionalUS,
		JobID: j.job.ID, State: state, Reason: j.reason, Attempts: j.attempts,
		ObservedUS: nowUS, DurableUS: j.durableUS,
		RemainingUS: j.job.UsefulUS - j.durableUS - provisionalUS,
	})
}

func (r *replayRun) finish(ctx context.Context, scheduler Scheduler, nowUS int64) error {
	for len(r.ends) > 0 && r.ends[0].completionUS == nowUS {
		j := r.ends[0].active
		if err := scheduler.Complete(ctx, nowUS, j.job.ID); err != nil {
			return err
		}
		if _, err := r.closeAttempt(j, nowUS, Completed, ""); err != nil {
			return err
		}
		if err := r.observe(j, nowUS, Completed, 0); err != nil {
			return err
		}
		delete(r.active, j.job.ID)
	}
	return nil
}

func (r *replayRun) apply(nowUS, endUS int64, decisions Decisions) error {
	slices.SortFunc(decisions.Preempt, func(a, b Preemption) int { return strings.Compare(a.JobID, b.JobID) })
	for _, preempted := range decisions.Preempt {
		j := r.active[preempted.JobID]
		if j == nil || j.attempt == nil {
			return fmt.Errorf("scheduler preempted job %q without a running attempt", preempted.JobID)
		}
		if _, err := r.closeAttempt(j, nowUS, Preempted, preempted.Reason); err != nil {
			return err
		}
	}
	slices.SortFunc(decisions.Allocate, func(a, b Placement) int { return strings.Compare(a.JobID, b.JobID) })
	for _, p := range decisions.Allocate {
		j := r.active[p.JobID]
		if j == nil || j.attempt != nil || j.reason != "" {
			return fmt.Errorf("scheduler allocated job %q without an eligible queued job", p.JobID)
		}
		if err := r.output.Queue(QueueInterval{Offered: j.job, QueuedUS: j.queuedUS, EndUS: nowUS}); err != nil {
			return err
		}
		if j.attempts == 0 {
			j.firstAllocationUS = &nowUS
		} else {
			j.retryWaitUS += nowUS - j.queuedUS
		}
		j.attempts++
		a := &attempt{active: j, placement: p, allocatedUS: nowUS, index: -1}
		j.attempt = a
		r.running++
		duration := j.job.StartupUS + j.job.UsefulUS - j.durableUS
		if duration < endUS-nowUS {
			a.completionUS = nowUS + duration
			heap.Push(&r.ends, a)
		}
	}
	return nil
}

func (r *replayRun) cutoff(ctx context.Context, nowUS int64) error {
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
			provisional, err = r.closeAttempt(j, nowUS, state, "")
			if err != nil {
				return err
			}
		}
		if state == Queued {
			if j.attempts > 0 {
				j.retryWaitUS += nowUS - j.queuedUS
			}
			if err := r.output.Queue(QueueInterval{Offered: j.job, QueuedUS: j.queuedUS, EndUS: nowUS, Censored: true}); err != nil {
				return err
			}
		}
		if err := r.observe(j, nowUS, state, provisional); err != nil {
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
		if hasOffered && (incoming.SubmittedUS < offered.SubmittedUS ||
			(incoming.SubmittedUS == offered.SubmittedUS && incoming.ID <= offered.ID)) {
			return fmt.Errorf("job %q arrived out of input order", incoming.ID)
		}
		offered, hasOffered = incoming, true
		stats.PeakLookahead = 1
		return nil
	}
	if err := read(); err != nil {
		return stats, err
	}
	for hasOffered && offered.SubmittedUS < window.WarmupUS {
		stats.BeforeWarmup++
		if err := read(); err != nil {
			return stats, err
		}
	}
	nextCycle := window.WarmupUS
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		now := nextCycle
		if hasOffered {
			now = min(now, offered.SubmittedUS)
		}
		if len(r.ends) > 0 {
			now = min(now, r.ends[0].completionUS)
		}
		if now >= window.EndUS {
			break
		}
		if err := r.finish(ctx, scheduler, now); err != nil {
			return stats, fmt.Errorf("complete at %d: %w", now, err)
		}
		for hasOffered && offered.SubmittedUS == now {
			if _, exists := r.active[offered.ID]; exists {
				return stats, fmt.Errorf("duplicate active job %q", offered.ID)
			}
			reason, err := scheduler.Admit(ctx, now, offered)
			if err != nil {
				return stats, fmt.Errorf("admit job %q: %w", offered.ID, err)
			}
			r.active[offered.ID] = &activeJob{job: offered, reason: reason, queuedUS: now}
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
			if err := r.apply(now, window.EndUS, decisions); err != nil {
				return stats, fmt.Errorf("apply schedule at %d: %w", now, err)
			}
			stats.PeakQueued = max(stats.PeakQueued, len(r.active)-r.running)
			stats.PeakRunning = max(stats.PeakRunning, r.running)
			stats.PeakCompletionEvents = max(stats.PeakCompletionEvents, len(r.ends))
			nextCycle += min(window.CycleUS, window.EndUS-now)
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
	return stats, r.cutoff(ctx, window.EndUS)
}
