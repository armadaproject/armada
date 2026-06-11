package runner

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
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
	GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*scheduling.SchedulerResult, error)

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

type gangKey struct {
	queue  string
	gangId string
}
