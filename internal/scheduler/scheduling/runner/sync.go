package runner

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

type syncSchedulingRunner struct {
	schedulingAlgo scheduling.SchedulingAlgo
}

func NewSyncSchedulingRunner(schedulingAlgo scheduling.SchedulingAlgo) SchedulingRunner {
	return &syncSchedulingRunner{schedulingAlgo: schedulingAlgo}
}

func (r *syncSchedulingRunner) Trigger() bool { return false }

func (r *syncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	return r.schedulingAlgo.Schedule(ctx, txn)
}

func (r *syncSchedulingRunner) IsAsync() bool { return false }

func (r *syncSchedulingRunner) Reset() {}
