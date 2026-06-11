package runner

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

// syncSchedulingRunner runs the underlying algorithm inline against the
// caller's txn. Trigger is a no-op because there is no background work.
type syncSchedulingRunner struct {
	schedulingAlgo scheduling.SchedulingAlgo
}

// NewSyncSchedulingRunner returns a runner that runs the algorithm inline on
// the caller's txn during GetSchedulerResult.
func NewSyncSchedulingRunner(schedulingAlgo scheduling.SchedulingAlgo) SchedulingRunner {
	return &syncSchedulingRunner{schedulingAlgo: schedulingAlgo}
}

func (r *syncSchedulingRunner) Trigger() {}

func (r *syncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	return r.schedulingAlgo.Schedule(ctx, resourceUnits, txn)
}

func (r *syncSchedulingRunner) IsAsync() bool { return false }

func (r *syncSchedulingRunner) Reset() {}
