package scheduling

import (
	"sync"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// SchedulingRunner drives a SchedulingAlgo through its sync or async lifecycle.
//
// The interface is the same in both modes so the caller's main loop has a
// single shape:
//
//	result, err := runner.GetSchedulerResult(ctx, resourceUnits, txn)
//	// ... build events, txn.Commit() ...
//	runner.Trigger()
//
// Sync impl runs the algo inline; Trigger is a no-op.
//
// Async impl owns a goroutine launched at construction and tied to the
// context passed to NewAsyncSchedulingRunner. GetSchedulerResult takes the
// pending background result and reconciles it against txn; Trigger requests
// the next run AFTER the caller has committed, so the next run's snapshot
// includes committed decisions.
type SchedulingRunner interface {
	// Trigger requests the next scheduling run.
	// Async: non-blocking send on the trigger channel; dropped if a run is in
	// flight or a result is pending. Call AFTER committing the reconciled txn.
	// Sync: no-op.
	Trigger()

	// GetSchedulerResult returns the scheduling result the caller should
	// apply against txn this cycle.
	// Sync: runs the underlying algo inline.
	// Async: takes the pending background result and reconciles it against
	// txn. Returns an empty SchedulerResult if no run has completed yet.
	GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*SchedulerResult, error)

	// IsAsync reports whether scheduling runs on a background goroutine.
	// Callers may need this to branch behaviour around the runner — e.g.
	// skipping pre-computation that the async impl ignores, or routing
	// timing metrics differently.
	IsAsync() bool
}

// syncSchedulingRunner runs the underlying algorithm inline against the
// caller's txn. Trigger is a no-op because there is no background work.
type syncSchedulingRunner struct {
	schedulingAlgo SchedulingAlgo
}

// NewSyncSchedulingRunner returns a runner that runs the algorithm inline on
// the caller's txn during GetSchedulerResult.
func NewSyncSchedulingRunner(schedulingAlgo SchedulingAlgo) SchedulingRunner {
	return &syncSchedulingRunner{schedulingAlgo: schedulingAlgo}
}

func (r *syncSchedulingRunner) Trigger() {}

func (r *syncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*SchedulerResult, error) {
	return r.schedulingAlgo.Schedule(ctx, resourceUnits, txn)
}

func (r *syncSchedulingRunner) IsAsync() bool { return false }

// asyncSchedulingRunner runs the algorithm on a background goroutine
// following the "combined runner" pattern (see experiment/combined_runner.go):
// the goroutine schedules against an isolated dry-run jobDb txn, and the
// caller pulls + reconciles the pending result via GetSchedulerResult and
// kicks the next run via Trigger.
//
// Rules:
//   - Trigger is dropped if the goroutine is busy or a result is pending.
//   - A new run will not start until the previous result has been taken.
//   - Trigger must be called after the caller commits its txn so the next
//     run's dry-run txn includes the reconciled decisions.
//
// TODO: resourceUnits is passed as nil to schedulingAlgo.Schedule below.
// In sync mode the caller computes resourceUnits via updateJobPrices against
// the real txn immediately before scheduling — the background goroutine
// cannot reproduce this. Market-driven scheduling will misprice (or fail)
// until this is solved. Options:
//   - Let pricing be updated async (possibly exclude jobs with no pricing info).
//   - Sort out the jobdb to set price info as jobs come in (how to handle
//     startup pricing?).
//
// This is a blocker for async mode being functionally complete; reconcile
// alone is not sufficient.
//
// TODO: metrics. The scheduler's ReportScheduleCycleTime currently measures
// the consume-and-reconcile cycle, not the actual scheduling work running on
// this goroutine. Needs reworking when async is enabled — likely time
// schedulingAlgo.Schedule inside run() and report from there, or add a
// separate async-scheduling-duration metric.
//
// # Open questions
//
// Errors
//   - Should we immediately retry on error or wait for the error to be consumed?
type asyncSchedulingRunner struct {
	schedulingAlgo SchedulingAlgo
	jobDb          *jobdb.JobDb

	trigger chan struct{}
	ready   chan struct{}

	mu     sync.Mutex
	result *scheduleResult
}

type scheduleResult struct {
	schedulerResult *SchedulerResult
	err             error
}

// NewAsyncSchedulingRunner returns a runner that schedules on a background
// goroutine bound to ctx. The goroutine exits when ctx is cancelled.
//
// The constructor blocks until the goroutine has signalled it is ready to
// receive triggers, so the first Trigger() never drops to a startup race.
func NewAsyncSchedulingRunner(ctx *armadacontext.Context, schedulingAlgo SchedulingAlgo, jobDb *jobdb.JobDb) SchedulingRunner {
	r := &asyncSchedulingRunner{
		schedulingAlgo: schedulingAlgo,
		jobDb:          jobDb,
		trigger:        make(chan struct{}), // unbuffered — triggers don't queue
		ready:          make(chan struct{}),
	}
	go r.run(ctx)
	<-r.ready
	return r
}

func (r *asyncSchedulingRunner) Trigger() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

func (r *asyncSchedulingRunner) GetSchedulerResult(_ *armadacontext.Context, _ map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*SchedulerResult, error) {
	r.mu.Lock()
	res := r.result
	r.result = nil
	r.mu.Unlock()

	if res == nil {
		return &SchedulerResult{}, nil
	}
	if res.err != nil {
		return nil, res.err
	}
	return r.reconcile(txn, res.schedulerResult)
}

func (r *asyncSchedulingRunner) IsAsync() bool { return true }

func (r *asyncSchedulingRunner) run(ctx *armadacontext.Context) {
	close(r.ready)
	for {
		select {
		case <-r.trigger:
		case <-ctx.Done():
			return
		}

		r.mu.Lock()
		skip := r.result != nil
		r.mu.Unlock()
		if skip {
			continue
		}

		txn := r.jobDb.DryRunTxn()
		result, err := r.schedulingAlgo.Schedule(ctx, nil, txn)

		r.mu.Lock()
		r.result = &scheduleResult{schedulerResult: result, err: err}
		r.mu.Unlock()
	}
}

// reconcile validates an async scheduling result against the current txn,
// dropping decisions that are no longer valid (e.g. job no longer pending,
// node no longer available, job no longer running for preemptions).
//
// TODO: implement. For each pool result, walk the SchedulingContext decisions
// and filter out stale ones (call MarkUnscheduled on entries that no longer
// hold) before returning.
func (r *asyncSchedulingRunner) reconcile(txn *jobdb.Txn, result *SchedulerResult) (*SchedulerResult, error) {
	_ = txn
	return result, nil
}
