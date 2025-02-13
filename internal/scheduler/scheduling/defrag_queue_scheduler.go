package scheduling

import (
	"math"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/scheduling/optimiser"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type DefragQueueScheduler struct {
	nodeScheduler                     *optimiser.FairnessOptimisingScheduler
	jobDb                             jobdb.JobRepository
	constraints                       schedulerconstraints.SchedulingConstraints
	floatingResourceTypes             *floatingresources.FloatingResourceTypes
	maxQueueLookBack                  uint
	minimumJobSizeToSchedule          *internaltypes.ResourceList
	maximumJobsToSchedule             int
	maximumResourceFractionToSchedule map[string]float64
}

func NewDefragQueueScheduler(jobDb jobdb.JobRepository, nodeScheduler *optimiser.FairnessOptimisingScheduler, maxQueueLookBack uint) *DefragQueueScheduler {
	return &DefragQueueScheduler{
		nodeScheduler:    nodeScheduler,
		jobDb:            jobDb,
		maxQueueLookBack: maxQueueLookBack,
	}
}

func (q *DefragQueueScheduler) Schedule(ctx *armadacontext.Context, sctx *schedulercontext.SchedulingContext) (*SchedulerResult, error) {
	gangIterator, err := q.createGangIterator(ctx, sctx)
	if err != nil {
		return nil, err
	}
	var scheduledJobs []*schedulercontext.JobSchedulingContext
	var preemptedJobs []*schedulercontext.JobSchedulingContext
	nodeIdByJobId := make(map[string]string)
	numberOfJobsScheduled := 0

	factory := sctx.TotalResources.Factory()
	maximumResourceFractionToSchedule := q.maximumResourceFractionToSchedule
	maximumResourceToSchedule := sctx.TotalResources.Multiply(factory.MakeResourceFractionList(maximumResourceFractionToSchedule, math.Inf(1)))
	currentResourceScheduled := factory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{})

	for numberOfJobsScheduled < q.maximumJobsToSchedule && !currentResourceScheduled.Exceeds(maximumResourceToSchedule) {
		// Peek() returns the next gang to try to schedule. Call Clear() before calling Peek() again.
		// Calling Clear() after (failing to) schedule ensures we get the next gang in order of smallest fair share.
		gctx, queueCostInclGang, err := gangIterator.Peek()
		if err != nil {
			sctx.TerminationReason = err.Error()
			return nil, err
		}
		if gctx == nil {
			break
		}

		ok, reason, err := q.checkIfWillBreachSchedulingLimits(gctx, sctx)
		if err != nil {
			sctx.TerminationReason = err.Error()
			return nil, err
		}
		if !ok {
			if schedulerconstraints.IsTerminalUnschedulableReason(reason) {
				// Global limit hit, exit
				break
			} else if schedulerconstraints.IsTerminalQueueUnschedulableReason(reason) {
				// If unschedulableReason indicates no more new jobs can be scheduled for this queue,
				// instruct the underlying iterator to only yield evicted jobs for this queue from now on.
				gangIterator.OnlyYieldEvictedForQueue(gctx.Queue)
			}
		}
		// Don't schedule jobs via defrag that'd put a queue over its fairshare
		if queueCostInclGang > 1.0 {
			// TODO consider how this should interact with unfeasible scheduling keys
			continue
		}

		if gctx.Cardinality() == 0 {
			if err := gangIterator.Clear(); err != nil {
				return nil, err
			}
			continue
		}
		alreadyScheduled := false
		// This is needed as we haven't marked scheduled jobs as scheduled in the job db yet, so we must exclude them here
		// TODO move optimiser to run after jobdb updated
		for _, jctx := range gctx.JobSchedulingContexts {
			if _, scheduled := sctx.QueueSchedulingContexts[gctx.Queue].SuccessfulJobSchedulingContexts[jctx.JobId]; scheduled {
				alreadyScheduled = true
				break
			}
		}

		if alreadyScheduled {
			if err := gangIterator.Clear(); err != nil {
				return nil, err
			}
			continue
		}
		select {
		case <-ctx.Done():
			// TODO: Better to push ctx into next and have that control it.
			err := ctx.Err()
			sctx.TerminationReason = err.Error()
			return nil, err
		default:
		}
		start := time.Now()
		scheduledOk, preemptedJctxs, unschedulableReason, err := q.nodeScheduler.Schedule(ctx, gctx, sctx)
		if err != nil {
			return nil, err
		} else if scheduledOk {
			for _, jctx := range gctx.JobSchedulingContexts {
				if pctx := jctx.PodSchedulingContext; pctx.IsSuccessful() {
					scheduledJobs = append(scheduledJobs, jctx)
					nodeIdByJobId[jctx.JobId] = pctx.NodeId
				}
			}
			preemptedJobs = append(preemptedJobs, preemptedJctxs...)
			numberOfJobsScheduled++
			currentResourceScheduled.Add(gctx.TotalResourceRequests)
		} else {
			q.updateUnfeasibleSchedulingKeys(gctx, sctx, unschedulableReason)
		}

		duration := time.Now().Sub(start)

		if duration.Seconds() > 1 {
			ctx.Infof("Slow schedule: queue %s, gang cardinality %d, sample job id %s, time %fs", gctx.Queue, gctx.Cardinality(), gctx.JobIds()[0], duration.Seconds())
		}

		// Clear() to get the next gang in order of smallest fair share.
		// Calling clear here ensures the gang scheduled in this iteration is accounted for.
		if err := gangIterator.Clear(); err != nil {
			return nil, err
		}
	}
	return &SchedulerResult{
		ScheduledJobs: scheduledJobs,
		NodeIdByJobId: nodeIdByJobId,
	}, nil
}

func (q *DefragQueueScheduler) createGangIterator(
	ctx *armadacontext.Context,
	sctx *schedulercontext.SchedulingContext) (CandidateGangIterator, error) {

	jobIteratorByQueue := make(map[string]JobContextIterator)
	for _, qctx := range sctx.QueueSchedulingContexts {
		// We only want to run defrag on queues that are failing to achieve their fairshare
		// So skip any queue at or above its fairshare
		actualShare := sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx)
		if actualShare >= qctx.AdjustedFairShare {
			continue
		}
		queueIt := NewQueuedJobsIterator(ctx, qctx.Queue, sctx.Pool, q.jobDb, jobdb.FairShareOrder)
		jobIteratorByQueue[qctx.Queue] = queueIt
	}

	// Reset the scheduling keys cache after evicting jobs.
	sctx.ClearUnfeasibleSchedulingKeys()

	gangIteratorsByQueue := make(map[string]*QueuedGangIterator, len(jobIteratorByQueue))
	for queue, it := range jobIteratorByQueue {
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, q.maxQueueLookBack, true)
	}

	// TODO review args + if we should also have market based here
	return NewCostBasedCandidateGangIterator(sctx.Pool, sctx, sctx.FairnessCostProvider, gangIteratorsByQueue, false, true)
}

func (q *DefragQueueScheduler) checkIfWillBreachSchedulingLimits(
	gctx *schedulercontext.GangSchedulingContext,
	sctx *schedulercontext.SchedulingContext) (bool, string, error) {

	ok, unschedulableReason, err := q.constraints.CheckRoundConstraints(sctx)
	if err != nil || !ok {
		return false, unschedulableReason, err
	}

	_, err = sctx.AddGangSchedulingContext(gctx)
	if err != nil {
		return false, "", err
	}

	ok, unschedulableReason, err = q.constraints.CheckJobConstraints(sctx, gctx)
	if err != nil || !ok {
		return false, unschedulableReason, err
	}

	ok, unschedulableReason = q.floatingResourceTypes.WithinLimits(sctx.Pool, sctx.Allocated)
	if !ok {
		return ok, unschedulableReason, nil
	}

	if _, err := sctx.EvictGang(gctx); err != nil {
		return false, "", err
	}

	return true, "", nil
}

func (q *DefragQueueScheduler) updateUnfeasibleSchedulingKeys(gctx *schedulercontext.GangSchedulingContext,
	sctx *schedulercontext.SchedulingContext, unschedulableReason string) {
	globallyUnschedulable := schedulerconstraints.UnschedulableReasonIsPropertyOfGang(unschedulableReason)

	// Register globally unfeasible scheduling keys.
	//
	// Only record unfeasible scheduling keys for single-job gangs.
	// Since a gang may be unschedulable even if all its members are individually schedulable.
	if gctx.Cardinality() == 1 && globallyUnschedulable {
		jctx := gctx.JobSchedulingContexts[0]
		schedulingKey, ok := jctx.SchedulingKey()
		if ok && schedulingKey != schedulerobjects.EmptySchedulingKey {
			if _, ok := sctx.UnfeasibleSchedulingKeys[schedulingKey]; !ok {
				// Keep the first jctx for each unfeasible schedulingKey.
				sctx.UnfeasibleSchedulingKeys[schedulingKey] = jctx
			}
		}
	}
}
