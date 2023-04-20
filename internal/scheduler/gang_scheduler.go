package scheduler

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
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
	if ResourceListAsWeightedApproximateFloat64(sctx.ResourceScarcity, sctx.TotalResources) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		return nil, errors.Errorf(
			"no resources with non-zero weight available for scheduling on any cluster: resource scarcity %v, total resources %v",
			sctx.ResourceScarcity, sctx.TotalResources,
		)
	}
	if ResourceListAsWeightedApproximateFloat64(sctx.ResourceScarcity, nodeDb.TotalResources()) == 0 {
		// This refers to the resources currently considered for schedling.
		return nil, errors.Errorf(
			"no resources with non-zero weight available for scheduling in NodeDb: resource scarcity %v, total resources %v",
			sctx.ResourceScarcity, nodeDb.TotalResources(),
		)
	}
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
		if unschedulableReason == "" {
			return
		}
		for _, jctx := range gctx.JobSchedulingContexts {
			sch.schedulingContext.EvictJob(jctx.Job)
			jctx.UnschedulableReason = unschedulableReason
		}
		sch.schedulingContext.AddGangSchedulingContext(gctx)
	}()
	sch.schedulingContext.AddGangSchedulingContext(gctx)
	if !allGangsJobsEvicted(gctx) {
		// Check that the job is large enough for this executor.
		// This check needs to be here, since it relates to a specific job.
		// Omit limit checks for evicted jobs to avoid preempting jobs if, e.g., MinimumJobSize changes.
		if ok, unschedulableReason = jobIsLargeEnough(gctx.TotalResourceRequests, sch.constraints.MinimumJobSize); !ok {
			return
		}
	}
	if ok, unschedulableReason, err = sch.constraints.CheckGlobalConstraints(ctx, sch.schedulingContext); err != nil || !ok {
		return
	}
	if ok, unschedulableReason, err = sch.constraints.CheckPerQueueAndPriorityClassConstraints(
		ctx,
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

// func (sch *GangScheduler) checkGlobalConstraints(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (bool, string, error) {
// 	if len(gctx.JobSchedulingContexts) == 0 {
// 		return true, "", nil
// 	}

// 	if sch.MaximumJobsToSchedule != 0 && sch.schedulingContext.NumScheduledJobs >= int(sch.MaximumJobsToSchedule) {
// 		return false, "maximum number of jobs scheduled", nil
// 	}
// 	if sch.MaximumGangsToSchedule != 0 && sch.schedulingContext.NumScheduledGangs >= int(sch.MaximumGangsToSchedule) {
// 		return false, "maximum number of gangs scheduled", nil
// 	}

// 	// Check that the job is large enough for this executor.
// 	if ok, unschedulableReason := jobIsLargeEnough(gctx.TotalResourceRequests, sch.MinimumJobSize); !ok {
// 		return false, unschedulableReason, nil
// 	}

// 	// MaximalResourceFractionToSchedule check.
// 	totalScheduledResources := sch.schedulingContext.ScheduledResourcesByPriority.AggregateByResource()
// 	totalScheduledResources.Add(gctx.TotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		totalScheduledResources,
// 		sch.schedulingContext.TotalResources,
// 		sch.MaximalResourceFractionToSchedule,
// 	); exceeded {
// 		unschedulableReason := reason + " (overall per scheduling round limit)"
// 		return false, unschedulableReason, nil
// 	}

// 	return true, "", nil
// }

// func (sch *GangScheduler) checkPerQueueConstraints(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (bool, string, error) {
// 	if len(gctx.JobSchedulingContexts) == 0 {
// 		return true, "", nil
// 	}

// 	// We assume that all jobs in a gang have the same priority class (which we enforce at job submission).
// 	// TODO: Add PC-checks.
// 	priority := int32(gctx.JobSchedulingContexts[0].Job.GetRequirements(sch.schedulingContext.PriorityClasses).Priority)
// 	qctx := sch.schedulingContext.QueueSchedulingContexts[gctx.Queue]
// 	if qctx == nil {
// 		return false, "", errors.Errorf("failed to schedule from queue %s: found no QueueSchedulingContext", gctx.Queue)
// 	}

// 	// MaximalResourceFractionToSchedulePerQueue check.
// 	roundQueueResourcesByPriority := qctx.ScheduledResourcesByPriority.DeepCopy()
// 	roundQueueResourcesByPriority.AddResourceList(priority, gctx.TotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		roundQueueResourcesByPriority.AggregateByResource(),
// 		sch.schedulingContext.TotalResources,
// 		sch.MaximalResourceFractionToSchedulePerQueue,
// 	); exceeded {
// 		unschedulableReason := reason + " (per scheduling round limit for this queue)"
// 		return false, unschedulableReason, nil
// 	}

// 	// MaximalResourceFractionPerQueue check.
// 	totalQueueResourcesByPriority := qctx.ResourcesByPriority.DeepCopy()
// 	totalQueueResourcesByPriority.AddResourceList(priority, gctx.TotalResourceRequests)
// 	if exceeded, reason := exceedsResourceLimits(
// 		ctx,
// 		totalQueueResourcesByPriority.AggregateByResource(),
// 		sch.schedulingContext.TotalResources,
// 		sch.MaximalResourceFractionPerQueue,
// 	); exceeded {
// 		unschedulableReason := reason + " (total limit for this queue)"
// 		return false, unschedulableReason, nil
// 	}

// 	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
// 	if exceeded, reason := exceedsPerPriorityResourceLimits(
// 		ctx,
// 		priority,
// 		totalQueueResourcesByPriority,
// 		sch.schedulingContext.TotalResources,
// 		sch.MaximalCumulativeResourceFractionPerQueueAndPriority,
// 	); exceeded {
// 		unschedulableReason := reason + " (total limit for this queue)"
// 		return false, unschedulableReason, nil
// 	}

// 	return true, "", nil
// }

func (sch *GangScheduler) trySchedule(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (bool, string, error) {
	pctxs, ok, err := sch.nodeDb.ScheduleMany(gctx.PodRequirements())
	if err != nil {
		return false, "", err
	}
	if len(pctxs) > len(gctx.JobSchedulingContexts) {
		return false, "", errors.Errorf("received %d pod scheduling context(s), but gang has cardinality %d", len(pctxs), len(gctx.JobSchedulingContexts))
	}
	for i, pctx := range pctxs {
		gctx.JobSchedulingContexts[i].PodSchedulingContext = pctx
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

// func queueFromGangSchedulingContext(gctx *schedulercontext.GangSchedulingContext) (string, error) {
// 	if len(gctx.JobSchedulingContexts) == 0 {
// 		return "", errors.New("gang has cardinality zero")
// 	}
// 	queue := gctx.JobSchedulingContexts[0].Job.GetQueue()
// 	for _, jctx := range gctx.JobSchedulingContexts {
// 		if jctx.Job.GetQueue() != queue {
// 			return "", errors.Errorf(
// 				"unexpected queue for job %s: expected %s but got %s",
// 				jctx.Job.GetId(), queue, jctx.Job.GetQueue(),
// 			)
// 		}
// 	}
// 	return queue, nil
// }

// Check that this job is at least equal to the minimum job size.
func jobIsLargeEnough(jobTotalResourceRequests, minimumJobSize schedulerobjects.ResourceList) (bool, string) {
	if len(minimumJobSize.Resources) == 0 {
		return true, ""
	}
	if len(jobTotalResourceRequests.Resources) == 0 {
		return true, ""
	}
	for resourceType, limit := range minimumJobSize.Resources {
		q := jobTotalResourceRequests.Get(resourceType)
		if limit.Cmp(q) == 1 {
			return false, fmt.Sprintf(
				"job requests %s %s, but the minimum is %s",
				q.String(), resourceType, limit.String(),
			)
		}
	}
	return true, ""
}
