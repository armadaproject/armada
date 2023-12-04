package scheduler

import (
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/armadaproject/armada/internal/common/armadacontext"
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

func (sch *GangScheduler) updateGangSchedulingContextOnSuccess(gctx *schedulercontext.GangSchedulingContext, gangAddedToSchedulingContext bool) error {
	if !gangAddedToSchedulingContext {
		// Nothing to do.
		return nil
	}

	// Evict any jobs added to the context marked as unsuccessful.
	// This is necessary to support min-max gang-scheduling,
	// where the gang is scheduled successfully if at least min of its members scheduled successfully.
	// Here, we evict the memebers of the gang that were not scheduled successfully.
	for _, jctx := range gctx.JobSchedulingContexts {
		if !jctx.IsSuccessful() {
			if _, err := sch.schedulingContext.EvictJob(jctx.Job); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sch *GangScheduler) updateGangSchedulingContextOnFailure(gctx *schedulercontext.GangSchedulingContext, gangAddedToSchedulingContext bool, unschedulableReason string) error {
	// If the job was added to the context, remove it first.
	if gangAddedToSchedulingContext {
		failedJobs := util.Map(gctx.JobSchedulingContexts, func(jctx *schedulercontext.JobSchedulingContext) interfaces.LegacySchedulerJob { return jctx.Job })
		if _, err := sch.schedulingContext.EvictGang(failedJobs); err != nil {
			return err
		}
	}

	// Ensure all jobs have an unschedulableReason.
	// Adding jobs with an unschedulableReason to the context ensures they're correctly accounted for as failed.
	for _, jctx := range gctx.JobSchedulingContexts {
		jctx.Fail(unschedulableReason)
	}
	if _, err := sch.schedulingContext.AddGangSchedulingContext(gctx); err != nil {
		return err
	}

	// Register unfeasible scheduling keys.
	//
	// Only record unfeasible scheduling keys for single-job gangs.
	// Since a gang may be unschedulable even if all its members are individually schedulable.
	if !sch.skipUnsuccessfulSchedulingKeyCheck && gctx.Cardinality() == 1 {
		jctx := gctx.JobSchedulingContexts[0]
		schedulingKey, ok := jctx.SchedulingKey()
		if ok && schedulingKey != schedulerobjects.EmptySchedulingKey {
			if _, ok := sch.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey]; !ok {
				// Keep the first jctx for each unfeasible schedulingKey.
				sch.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey] = jctx
			}
		}
	}

	return nil
}

func (sch *GangScheduler) Schedule(ctx *armadacontext.Context, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	// Exit immediately if this is a new gang and we've hit any round limits.
	if !gctx.AllJobsEvicted {
		if ok, unschedulableReason, err = sch.constraints.CheckRoundConstraints(sch.schedulingContext, gctx.Queue); err != nil || !ok {
			return
		}
	}

	// This deferred function ensures unschedulable jobs are registered as such.
	gangAddedToSchedulingContext := false
	defer func() {
		// Do nothing if an error occurred.
		if err != nil {
			return
		}

		// Update rate-limiters to account for new successfully scheduled jobs.
		if ok && !gctx.AllJobsEvicted {
			sch.schedulingContext.Limiter.ReserveN(sch.schedulingContext.Started, gctx.Cardinality())
			if qctx := sch.schedulingContext.QueueSchedulingContexts[gctx.Queue]; qctx != nil {
				qctx.Limiter.ReserveN(sch.schedulingContext.Started, gctx.Cardinality())
			}
		}

		if ok {
			err = sch.updateGangSchedulingContextOnSuccess(gctx, gangAddedToSchedulingContext)
		} else {
			err = sch.updateGangSchedulingContextOnFailure(gctx, gangAddedToSchedulingContext, unschedulableReason)
		}
	}()

	if _, err = sch.schedulingContext.AddGangSchedulingContext(gctx); err != nil {
		return
	}
	gangAddedToSchedulingContext = true
	if !gctx.AllJobsEvicted {
		// Only perform these checks for new jobs to avoid preempting jobs if, e.g., MinimumJobSize changes.
		if ok, unschedulableReason, err = sch.constraints.CheckConstraints(sch.schedulingContext, gctx); err != nil || !ok {
			return
		}
	}
	return sch.trySchedule(ctx, gctx)
}

func (sch *GangScheduler) trySchedule(ctx *armadacontext.Context, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
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
	bestFit := schedulercontext.GangSchedulingFit{}
	i := 0
	for value := range nodeUniformityLabelValues {
		i++
		if value == "" {
			continue
		}
		addNodeSelectorToGctx(gctx, gctx.NodeUniformityLabel, value)
		txn := sch.nodeDb.Txn(true)
		ok, unschedulableReason, err = sch.tryScheduleGangWithTxn(ctx, txn, gctx)
		if err != nil {
			txn.Abort()
			return
		}
		if ok {
			currentFit := gctx.Fit()
			if currentFit.NumScheduled == gctx.Cardinality() && currentFit.MeanPreemptedAtPriority == float64(nodedb.MinPriority) {
				// Best possible; no need to keep looking.
				txn.Commit()
				return true, "", nil
			}
			if bestValue == "" || bestFit.Less(currentFit) {
				if i == len(nodeUniformityLabelValues) {
					// Minimal meanScheduledAtPriority and no more options; commit and return.
					txn.Commit()
					return true, "", nil
				}
				// Record the best value seen so far.
				bestValue = value
				bestFit = currentFit
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

func (sch *GangScheduler) tryScheduleGang(ctx *armadacontext.Context, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	txn := sch.nodeDb.Txn(true)
	defer txn.Abort()
	ok, unschedulableReason, err = sch.tryScheduleGangWithTxn(ctx, txn, gctx)
	if ok && err == nil {
		txn.Commit()
	}
	return
}

func (sch *GangScheduler) tryScheduleGangWithTxn(_ *armadacontext.Context, txn *memdb.Txn, gctx *schedulercontext.GangSchedulingContext) (ok bool, unschedulableReason string, err error) {
	if ok, err = sch.nodeDb.ScheduleManyWithTxn(txn, gctx.JobSchedulingContexts); err == nil {
		if !ok {
			if gctx.Cardinality() > 1 {
				unschedulableReason = "unable to schedule gang since minimum cardinality not met"
			} else {
				unschedulableReason = "job does not fit on any node"
			}
		} else {
			// When a gang schedules successfully, update state for failed jobs if they exist.
			for _, jctx := range gctx.JobSchedulingContexts {
				if jctx.ShouldFail {
					jctx.Fail("job does not fit on any node")
				}
			}
		}

		return
	}

	return
}

func addNodeSelectorToGctx(gctx *schedulercontext.GangSchedulingContext, nodeSelectorKey, nodeSelectorValue string) {
	for _, jctx := range gctx.JobSchedulingContexts {
		jctx.AddNodeSelector(nodeSelectorKey, nodeSelectorValue)
	}
}
