package scheduling

import (
	"context"
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
	// Async: dropped if a run is in flight, requested, or a result is
	// pending. Call AFTER committing the reconciled txn.
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

	// Reset cancels any in-flight scheduling and discards any pending
	// result. Blocks until the in-flight run (if any) has actually
	// returned. Intended to be called on leader-takeover so the new leader
	// can be sure no result it later consumes was started under the
	// previous leader's tenure.
	// Sync: no-op.
	Reset()
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

func (r *syncSchedulingRunner) Reset() {}

// runState tracks the lifecycle of a scheduling run.
//
//	stateIdle      → no work pending or in flight
//	stateRequested → Trigger has been called; goroutine has not yet picked it up
//	stateRunning   → goroutine is inside schedulingAlgo.Schedule
type runState int

const (
	stateIdle runState = iota
	stateRequested
	stateRunning
)

// asyncSchedulingRunner runs the algorithm on a background goroutine
// following the "combined runner" pattern (see experiment/combined_runner.go):
// the goroutine schedules against an isolated dry-run jobDb txn, and the
// caller pulls + reconciles the pending result via GetSchedulerResult and
// kicks the next run via Trigger.
//
// Concurrency model: all lifecycle state (state, result, cancelFn, runDone)
// lives under mu. The wake channel is a pure signal — it carries no meaning;
// the goroutine reads state under mu after each wake. This makes Trigger and
// Reset atomic with respect to the goroutine's own transitions, eliminating
// any race window between "goroutine has consumed a trigger" and "goroutine
// has registered as in-flight".
//
// Rules:
//   - Trigger is dropped if state != idle or a result is pending.
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

	// wake is a size-1 buffered channel used purely to nudge the goroutine
	// out of its wait. State is in mu; the channel carries no meaning.
	wake chan struct{}

	mu sync.Mutex
	// state is the lifecycle position of the runner.
	state runState
	// result of the last completed background run, awaiting consumption.
	result *scheduleResult
	// cancelFn cancels the ctx of the in-flight Schedule call. Non-nil
	// only while state == stateRunning.
	cancelFn context.CancelFunc
	// runDone is closed by the goroutine when the in-flight run has
	// returned (either normally or due to cancellation). Non-nil only
	// while state == stateRunning.
	runDone chan struct{}
}

type scheduleResult struct {
	schedulerResult *SchedulerResult
	err             error
}

// NewAsyncSchedulingRunner returns a runner that schedules on a background
// goroutine bound to ctx. The goroutine exits when ctx is cancelled.
func NewAsyncSchedulingRunner(ctx *armadacontext.Context, schedulingAlgo SchedulingAlgo, jobDb *jobdb.JobDb) SchedulingRunner {
	r := &asyncSchedulingRunner{
		schedulingAlgo: schedulingAlgo,
		jobDb:          jobDb,
		wake:           make(chan struct{}, 1),
	}
	go r.run(ctx)
	return r
}

func (r *asyncSchedulingRunner) Trigger() {
	r.mu.Lock()
	busy := r.state != stateIdle || r.result != nil
	if !busy {
		r.state = stateRequested
	}
	r.mu.Unlock()
	if busy {
		return
	}
	select {
	case r.wake <- struct{}{}:
	default:
		// wake already buffered; the goroutine will see state on its next
		// read regardless.
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

// Reset discards any pending result, cancels any in-flight Schedule call,
// and blocks until that call has returned. After Reset returns, the next
// Trigger starts a fresh run.
//
// Because state lives under mu, there is no window between "goroutine
// consumed a trigger" and "goroutine registered as in-flight" — those
// happen under the same lock. Reset's view of the runner is consistent.
func (r *asyncSchedulingRunner) Reset() {
	r.mu.Lock()
	cancel := r.cancelFn
	done := r.runDone
	r.cancelFn = nil
	r.runDone = nil
	r.result = nil
	r.state = stateIdle
	r.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
}

func (r *asyncSchedulingRunner) run(ctx *armadacontext.Context) {
	for {
		// Wait for state == stateRequested. State changes only happen under
		// mu, so the goroutine is guaranteed to see Trigger / Reset
		// transitions atomically.
		r.mu.Lock()
		for r.state != stateRequested {
			r.mu.Unlock()
			select {
			case <-r.wake:
			case <-ctx.Done():
				return
			}
			r.mu.Lock()
		}

		// Transition stateRequested → stateRunning under the same lock that
		// observed stateRequested, so Reset cannot observe an "in between" state.
		r.state = stateRunning
		runCtx, cancel := armadacontext.WithCancel(ctx)
		done := make(chan struct{})
		r.cancelFn = cancel
		r.runDone = done
		r.mu.Unlock()

		txn := r.jobDb.DryRunTxn()
		result, err := r.schedulingAlgo.Schedule(runCtx, nil, txn)

		r.mu.Lock()
		// If r.cancelFn is still our cancel, Reset hasn't fired and this is
		// a normal completion — store the result and return to idle.
		// Otherwise, Reset has cleared the fields and forced state to idle;
		// drop the result and don't touch state.
		if r.cancelFn != nil {
			r.result = &scheduleResult{schedulerResult: result, err: err}
			r.cancelFn = nil
			r.runDone = nil
			r.state = stateIdle
		}
		r.mu.Unlock()
		cancel()
		close(done)
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
