package runner

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

type syncSchedulingRunner struct {
	schedulingAlgo scheduling.SchedulingAlgo
}

func NewSyncSchedulingRunner(schedulingAlgo scheduling.SchedulingAlgo) SchedulingRunner {
	return &syncSchedulingRunner{schedulingAlgo: schedulingAlgo}
}

func (r *syncSchedulingRunner) Trigger() {}

func (r *syncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, resourceUnits map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	return r.schedulingAlgo.Schedule(ctx, resourceUnits, txn)
}

func (r *syncSchedulingRunner) IsAsync() bool { return false }

func (r *syncSchedulingRunner) Reset() {}
