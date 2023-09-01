package scheduler

import (
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/armadaproject/armada/internal/common/context"
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
	// If true, the unsuccessfulSchedulingKeys check is omitted.
	skipUnsuccessfulSchedulingKeyCheck bool
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

func (sch *GangScheduler) SkipUnsuccessfulSchedulingKeyCheck() {
	sch.skipUnsuccessfulSchedulingKeyCheck = true
}

func (sch *GangScheduler) Schedule(ctx *context.ArmadaContext, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
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
				if _, err = sch.schedulingContext.EvictGang(jobs); err != nil {
					return
				}
			}
			for _, jctx := range gctx.JobSchedulingContexts {
				jctx.UnschedulableReason = unschedulableReason
			}
			if _, err = sch.schedulingContext.AddGangSchedulingContext(gctx); err != nil {
				return
			}

			// Register unfeasible scheduling keys.
			//
			// Only record unfeasible scheduling keys for single-job gangs.
			// Since a gang may be unschedulable even if all its members are individually schedulable.
			if !sch.skipUnsuccessfulSchedulingKeyCheck && len(gctx.JobSchedulingContexts) == 1 {
				jctx := gctx.JobSchedulingContexts[0]
				schedulingKey := sch.schedulingContext.SchedulingKeyFromLegacySchedulerJob(jctx.Job)
				if _, ok := sch.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey]; !ok {
					// Keep the first jctx for each unfeasible schedulingKey.
					sch.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey] = jctx
				}
			}
		}
	}()

	// Try scheduling the gang.
	if _, err = sch.schedulingContext.AddGangSchedulingContext(gctx); err != nil {
		return
	}
	gangAddedToSchedulingContext = true
	if !gctx.AllJobsEvicted {
		// Check that the job is large enough for this executor.
		// This check needs to be here, since it relates to a specific job.
		// Only perform limit checks for new jobs to avoid preempting jobs if, e.g., MinimumJobSize changes.
		if ok, unschedulableReason = requestsAreLargeEnough(gctx.TotalResourceRequests, sch.constraints.MinimumJobSize); !ok {
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
	return sch.trySchedule(ctx, gctx)
}

func (sch *GangScheduler) trySchedule(ctx *context.ArmadaContext, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	// If no node uniformity constraint, try scheduling across all nodes.
	if gctx.NodeUniformityLabel == "" {
		return sch.tryScheduleGang(ctx, gctx)
	}

	// Otherwise try scheduling such that all nodes onto which a gang job lands have the same value for gctx.NodeUniformityLabel.
	// We do this by making a separate scheduling attempt for each unique value of gctx.NodeUniformityLabel.
	nodeUniformityLabelValues, ok := sch.nodeDb.IndexedNodeLabelValues(gctx.NodeUniformityLabel)
	if !ok {
		ok = false
		unschedulableReason = fmt.Sprintf("uniformity label %s is not indexed", gctx.NodeUniformityLabel)
		return
	}
	if len(nodeUniformityLabelValues) == 0 {
		ok = false
		unschedulableReason = fmt.Sprintf("no nodes with uniformity label %s", gctx.NodeUniformityLabel)
		return
	}

	// Try all possible values of nodeUniformityLabel one at a time to find the best fit.
	bestValue := ""
	var minMeanScheduledAtPriority float64
	var i int
	for value := range nodeUniformityLabelValues {
		i++
		if value == "" {
			continue
		}
		addNodeSelectorToGctx(gctx, gctx.NodeUniformityLabel, value)
		txn := sch.nodeDb.Txn(true)
		if ok, unschedulableReason, err = sch.tryScheduleGangWithTxn(ctx, txn, gctx); err != nil {
			txn.Abort()
			return
		} else if ok {
			meanScheduledAtPriority, ok := meanScheduledAtPriorityFromGctx(gctx)
			if !ok {
				txn.Abort()
				continue
			}
			if meanScheduledAtPriority == float64(nodedb.MinPriority) {
				// Best possible; no need to keep looking.
				txn.Commit()
				return true, "", nil
			}
			if bestValue == "" || meanScheduledAtPriority <= minMeanScheduledAtPriority {
				if i == len(nodeUniformityLabelValues) {
					// Minimal meanScheduledAtPriority and no more options; commit and return.
					txn.Commit()
					return true, "", nil
				}
				// Record the best value seen so far.
				bestValue = value
				minMeanScheduledAtPriority = meanScheduledAtPriority
			}
		}
		txn.Abort()
	}
	if bestValue == "" {
		ok = false
		unschedulableReason = "at least one job in the gang does not fit on any node"
		return
	}
	addNodeSelectorToGctx(gctx, gctx.NodeUniformityLabel, bestValue)
	return sch.tryScheduleGang(ctx, gctx)
}

func (sch *GangScheduler) tryScheduleGang(ctx *context.ArmadaContext, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	txn := sch.nodeDb.Txn(true)
	defer txn.Abort()
	ok, unschedulableReason, err = sch.tryScheduleGangWithTxn(ctx, txn, gctx)
	if ok && err == nil {
		txn.Commit()
	}
	return
}

func (sch *GangScheduler) tryScheduleGangWithTxn(ctx *context.ArmadaContext, txn *memdb.Txn, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	if ok, err = sch.nodeDb.ScheduleManyWithTxn(txn, gctx.JobSchedulingContexts); err != nil {
		return
	} else if !ok {
		for _, jctx := range gctx.JobSchedulingContexts {
			if jctx.PodSchedulingContext != nil {
				// Clear any node bindings on failure to schedule.
				jctx.PodSchedulingContext.NodeId = ""
			}
		}
		if len(gctx.JobSchedulingContexts) > 1 {
			unschedulableReason = "at least one job in the gang does not fit on any node"
		} else {
			unschedulableReason = "job does not fit on any node"
		}
		return
	}
	return
}

func addNodeSelectorToGctx(gctx *schedulercontext.GangSchedulingContext, nodeSelectorKey, nodeSelectorValue string) {
	for _, jctx := range gctx.JobSchedulingContexts {
		if jctx.PodRequirements.NodeSelector == nil {
			jctx.PodRequirements.NodeSelector = make(map[string]string)
		}
		jctx.PodRequirements.NodeSelector[nodeSelectorKey] = nodeSelectorValue
	}
}

func meanScheduledAtPriorityFromGctx(gctx *schedulercontext.GangSchedulingContext) (float64, bool) {
	var sum int32
	for _, jctx := range gctx.JobSchedulingContexts {
		if jctx.PodSchedulingContext == nil {
			return 0, false
		}
		sum += jctx.PodSchedulingContext.ScheduledAtPriority
	}
	return float64(sum) / float64(len(gctx.JobSchedulingContexts)), true
}

func requestsAreLargeEnough(totalResourceRequests, minRequest schedulerobjects.ResourceList) (bool, string) {
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
