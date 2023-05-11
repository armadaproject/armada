package scheduler

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/util"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// GangScheduler schedules one gang at a time. GangScheduler is not aware of queues.
type GangScheduler struct {
	constraints       schedulerconstraints.SchedulingConstraints
	schedulingContext *schedulercontext.SchedulingContext
	nodeDb            *nodedb.NodeDb
}

func NewGangScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	nodeDb *nodedb.NodeDb,
) (*GangScheduler, error) {
	return &GangScheduler{
		constraints:       constraints,
		schedulingContext: sctx,
		nodeDb:            nodeDb,
	}, nil
}

func (sch *GangScheduler) Schedule(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	defer func() {
		// If any job in a gang fails to schedule, set the unschedulableReason for all jobs in the gang,
		// and remove it from the scheduling context.
		if err == nil && !ok {
			jobs := util.Map(gctx.JobSchedulingContexts, func(jctx *schedulercontext.JobSchedulingContext) interfaces.LegacySchedulerJob { return jctx.Job })
			sch.schedulingContext.EvictGang(jobs)
			for _, jctx := range gctx.JobSchedulingContexts {
				jctx.UnschedulableReason = unschedulableReason
			}
			sch.schedulingContext.AddGangSchedulingContext(gctx)
		}
	}()
	sch.schedulingContext.AddGangSchedulingContext(gctx)
	if !allGangsJobsEvicted(gctx) {
		// Check that the job is large enough for this executor.
		// This check needs to be here, since it relates to a specific job.
		// Omit limit checks for evicted jobs to avoid preempting jobs if, e.g., MinimumJobSize changes.
		if ok, unschedulableReason = requestIsLargeEnough(gctx.TotalResourceRequests, sch.constraints.MinimumJobSize); !ok {
			return
		}
	}
	if ok, unschedulableReason, err = sch.constraints.CheckGlobalConstraints(
		sch.schedulingContext,
	); err != nil || !ok {
		return
	}
	if ok, unschedulableReason, err = sch.constraints.CheckPerQueueAndPriorityClassConstraints(
		sch.schedulingContext,
		gctx.Queue,
		gctx.PriorityClassName,
	); err != nil || !ok {
		return
	}
	if ok, unschedulableReason, err = sch.trySchedule(ctx, gctx); err != nil || ok {
		return
	}
	return
}

func (sch *GangScheduler) trySchedule(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (bool, string, error) {
	pctxs, ok, err := sch.nodeDb.ScheduleMany(gctx.PodRequirements())
	if err != nil {
		return false, "", err
	}
	if len(pctxs) > len(gctx.JobSchedulingContexts) {
		return false, "", errors.Errorf(
			"received %d pod scheduling context(s), but gang has cardinality %d",
			len(pctxs), len(gctx.JobSchedulingContexts),
		)
	}
	for i, pctx := range pctxs {
		gctx.JobSchedulingContexts[i].PodSchedulingContext = pctx
		gctx.JobSchedulingContexts[i].NumNodes = pctx.NumNodes
	}
	if !ok {
		unschedulableReason := ""
		if len(gctx.JobSchedulingContexts) > 1 {
			unschedulableReason = "at least one job in the gang does not fit on any node"
		} else {
			unschedulableReason = "job does not fit on any node"
		}
		return false, unschedulableReason, nil
	}
	return true, "", nil
}

func allGangsJobsEvicted(gctx *schedulercontext.GangSchedulingContext) bool {
	rv := true
	for _, jctx := range gctx.JobSchedulingContexts {
		rv = rv && isEvictedJob(jctx.Job)
	}
	return rv
}

func requestIsLargeEnough(totalResourceRequests, minRequest schedulerobjects.ResourceList) (bool, string) {
	if len(minRequest.Resources) == 0 {
		return true, ""
	}
	for t, minQuantity := range minRequest.Resources {
		q := totalResourceRequests.Get(t)
		if minQuantity.Cmp(q) == 1 {
			return false, fmt.Sprintf("job requests %s %s, but the minimum is %s", q.String(), t, minQuantity.String())
		}
	}
	return true, ""
}
