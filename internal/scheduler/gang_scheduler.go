package scheduler

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
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
	// Record of job scheduling requirements of jobs that previously failed to schedule.
	// Used to immediately reject new jobs with identical reqirements.
	unsuccessfulSchedulingKeys map[schedulerobjects.SchedulingKey]*schedulercontext.JobSchedulingContext
	// If true, the unsuccessfulSchedulingKeys check is omitted.
	skipUnsuccessfulSchedulingKeyCheck bool
}

func NewGangScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	nodeDb *nodedb.NodeDb,
) (*GangScheduler, error) {
	return &GangScheduler{
		constraints:                constraints,
		schedulingContext:          sctx,
		nodeDb:                     nodeDb,
		unsuccessfulSchedulingKeys: make(map[schedulerobjects.SchedulingKey]*schedulercontext.JobSchedulingContext),
	}, nil
}

func (sch *GangScheduler) SkipUnsuccessfulSchedulingKeyCheck() {
	sch.skipUnsuccessfulSchedulingKeyCheck = true
}

func (sch *GangScheduler) Schedule(ctx context.Context, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	// Exit immediately if this is a new gang and we've hit any round limits.
	if !gctx.AllJobsEvicted {
		if ok, unschedulableReason, err = sch.constraints.CheckRoundConstraints(sch.schedulingContext); err != nil || !ok {
			return
		}
	}

	// This deferred function ensures unschedulable jobs are registered as such
	// and sets sch.queueScheduledInPreviousCall.
	gangAddedToSchedulingContext := false
	defer func() {
		// Do nothing if an error occurred.
		if err != nil {
			return
		}
		if !ok {
			// Register the job as unschedulable. If the job was added to the context, remove it first.
			if gangAddedToSchedulingContext {
				jobs := util.Map(gctx.JobSchedulingContexts, func(jctx *schedulercontext.JobSchedulingContext) interfaces.LegacySchedulerJob { return jctx.Job })
				sch.schedulingContext.EvictGang(jobs)
			}
			for _, jctx := range gctx.JobSchedulingContexts {
				jctx.UnschedulableReason = unschedulableReason
			}
			sch.schedulingContext.AddGangSchedulingContext(gctx)

			// Register unfeasible scheduling keys.
			//
			// Only record unfeasible scheduling keys for single-job gangs.
			// Since a gang may be unschedulable even if all its members are individually schedulable.
			if !sch.skipUnsuccessfulSchedulingKeyCheck {
				if schedulingKey, jctx, ok := schedulingKeyIfSingleJobGang(gctx, sch.schedulingContext.PriorityClasses); ok {
					if _, ok := sch.unsuccessfulSchedulingKeys[schedulingKey]; !ok {
						// Keep the first jctx for each unique schedulingKey.
						sch.unsuccessfulSchedulingKeys[schedulingKey] = jctx
					}
				}
			}
		}
	}()

	// Try scheduling the gang.
	sch.schedulingContext.AddGangSchedulingContext(gctx)
	gangAddedToSchedulingContext = true
	if !gctx.AllJobsEvicted {
		// Check that the job is large enough for this executor.
		// This check needs to be here, since it relates to a specific job.
		// Only perform limit checks for new jobs to avoid preempting jobs if, e.g., MinimumJobSize changes.
		if ok, unschedulableReason = requestIsLargeEnough(gctx.TotalResourceRequests, sch.constraints.MinimumJobSize); !ok {
			return
		}
		if ok, unschedulableReason, err = sch.constraints.CheckPerQueueAndPriorityClassConstraints(
			sch.schedulingContext,
			gctx.Queue,
			gctx.PriorityClassName,
		); err != nil || !ok {
			return
		}
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

func schedulingKeyIfSingleJobGang(
	gctx *schedulercontext.GangSchedulingContext,
	priorityClasses map[string]configuration.PriorityClass,
) (schedulerobjects.SchedulingKey, *schedulercontext.JobSchedulingContext, bool) {
	if len(gctx.JobSchedulingContexts) == 1 {
		jctx := gctx.JobSchedulingContexts[0]
		schedulingKey, ok := schedulingKeyFromLegacySchedulerJob(jctx.Job, priorityClasses)
		return schedulingKey, jctx, ok
	}
	return schedulerobjects.SchedulingKey{}, nil, false
}

func schedulingKeyFromLegacySchedulerJob(job interfaces.LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) (schedulerobjects.SchedulingKey, bool) {
	jobSchedulingInfo := job.GetRequirements(priorityClasses)
	schedulingKey, ok := jobSchedulingInfo.SchedulingKey()
	return schedulingKey, ok
}
