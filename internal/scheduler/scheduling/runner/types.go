package runner

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

// SchedulingRunner drives a SchedulingAlgo through its sync or async lifecycle.
//
// It is expected the jobdb is updated with the latest result of GetSchedulerResult before calling Trigger again
// - Failing to do so will result in scheduling on stale data.
type SchedulingRunner interface {
	// Trigger starts the next scheduling run in async runners.
	Trigger()

	// GetSchedulerResult
	// Applies the result of the current scheduling cycle to the provided txn.
	// If no scheduling result is ready (for async runners), nil will be returned.
	GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*scheduling.SchedulerResult, error)

	// IsAsync reports whether scheduling runs on a background goroutine.
	IsAsync() bool

	// Reset
	// Cancels any in-flight scheduling and discards any pending result.
	// Blocks until the in-flight run (if any) has actually returned
	Reset()
}
