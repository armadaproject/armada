package scheduling

import (
	"fmt"
	"math"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/optimiser"
)

type OptimisingQueueScheduler struct {
	gangScheduler                     optimiser.GangScheduler
	jobDb                             jobdb.JobRepository
	constraints                       schedulerconstraints.SchedulingConstraints
	floatingResourceTypes             *floatingresources.FloatingResourceTypes
	maxQueueLookBack                  uint
	minimumJobSizeToSchedule          *internaltypes.ResourceList
	maximumJobsToSchedule             int
	maximumResourceFractionToSchedule map[string]float64
	prioritiseLargerJobs              bool
}

func NewOptimisingQueueScheduler(
	jobDb jobdb.JobRepository,
	optimisingScheduler optimiser.GangScheduler,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	maxQueueLookBack uint,
	prioritiseLargerJobs bool,
	minimumJobSizeToSchedule *internaltypes.ResourceList,
	maximumJobsToSchedule int,
	maximumResourceFractionToSchedule map[string]float64,
) *OptimisingQueueScheduler {
	return &OptimisingQueueScheduler{
		gangScheduler:                     optimisingScheduler,
		jobDb:                             jobDb,
		maxQueueLookBack:                  maxQueueLookBack,
		prioritiseLargerJobs:              prioritiseLargerJobs,
		constraints:                       constraints,
		floatingResourceTypes:             floatingResourceTypes,
		minimumJobSizeToSchedule:          minimumJobSizeToSchedule,
		maximumJobsToSchedule:             maximumJobsToSchedule,
		maximumResourceFractionToSchedule: maximumResourceFractionToSchedule,
	}
}

// Schedule
// This is the entrypoint the optimiser
//   - It iterates through unscheduled (not evicted) jobs of queues below their fairshare
//   - It them makes some basic checks (rate limits etc)
//   - If the Job won't put the queue above its faishare and passes the checks, it off to the gangScheduler to schedule the job
func (q *OptimisingQueueScheduler) Schedule(ctx *armadacontext.Context, sctx *schedulercontext.SchedulingContext) (*SchedulerResult, error) {
	gangIterator, err := q.createCandidateGangIterator(ctx, sctx)
	if err != nil {
		return nil, err
	}
	scheduledJobs := []*schedulercontext.JobSchedulingContext{}
	preemptedJobs := []*schedulercontext.JobSchedulingContext{}
	nodeIdByJobId := make(map[string]string)

	factory := sctx.TotalResources.Factory()
	maximumResourceFractionToSchedule := q.maximumResourceFractionToSchedule
	maximumResourceToSchedule := sctx.TotalResources.Multiply(factory.MakeResourceFractionList(maximumResourceFractionToSchedule, math.Inf(1)))
	currentResourceScheduled := factory.MakeAllZero()
	start := time.Now()
	loopNumber := 0

	firstLoop := true

loop:
	for len(scheduledJobs) < q.maximumJobsToSchedule {
		if !firstLoop {
			if err := gangIterator.Clear(); err != nil {
				return nil, err
			}
		}
		firstLoop = false

		// Peek() returns the next gang to try to schedule. Call Clear() before calling Peek() again.
		// Calling Clear() after (failing to) schedule ensures we get the next gang in order of smallest fair share.
		gctx, queueCostInclGang, err := gangIterator.Peek()
		if err != nil {
			return nil, err
		}
		if gctx == nil {
			break
		}
		if gctx.Cardinality() == 0 {
			continue
		}
		if gctx.AllJobsEvicted {
			continue
		}

		qctx, exists := sctx.QueueSchedulingContexts[gctx.Queue]
		if !exists {
			return nil, fmt.Errorf("unable to find queue scheduling context for queue %s", gctx.Queue)
		}

		if queueCostInclGang > qctx.DemandCappedAdjustedFairShare/qctx.Weight {
			continue
		}

		// This is needed as we haven't marked scheduled jobs as scheduled in the job db yet, so we must exclude them here
		for _, jctx := range gctx.JobSchedulingContexts {
			if _, scheduled := sctx.QueueSchedulingContexts[gctx.Queue].SuccessfulJobSchedulingContexts[jctx.JobId]; scheduled {
				continue loop
			}
			if q.minimumJobSizeToSchedule != nil && q.minimumJobSizeToSchedule.Exceeds(jctx.Job.AllResourceRequirements()) {
				// Don't schedule jobs smaller than the minimum size
				continue loop
			}
		}

		if currentResourceScheduled.Add(gctx.TotalResourceRequests).Exceeds(maximumResourceToSchedule) {
			continue
		}

		ok, reason, err := q.checkIfWillBreachSchedulingLimits(gctx, sctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			if schedulerconstraints.IsTerminalUnschedulableReason(reason) {
				// Stop iterating if global limit hit
				break
			} else if schedulerconstraints.IsTerminalQueueUnschedulableReason(reason) {
				// If unschedulableReason indicates no more new jobs can be scheduled for this queue,
				// instruct the underlying iterator to skip the rest of this queue
				gangIterator.OnlyYieldEvictedForQueue(gctx.Queue)
			}
			continue
		}
		if hasContextExpired(ctx) || isNearDeadline(ctx) {
			break
		}
		start := time.Now()
		scheduledOk, preemptedJctxs, unschedulableReason, err := q.gangScheduler.Schedule(ctx, gctx, sctx)
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
			currentResourceScheduled = currentResourceScheduled.Add(gctx.TotalResourceRequests)

			// Update rate limiters
			sctx.Limiter.ReserveN(sctx.Started, gctx.Cardinality())
			if qctx := sctx.QueueSchedulingContexts[gctx.Queue]; qctx != nil {
				qctx.Limiter.ReserveN(sctx.Started, gctx.Cardinality())
			}
		} else {
			q.updateUnfeasibleSchedulingKeys(gctx, sctx, unschedulableReason)
		}

		duration := time.Now().Sub(start)

		if duration.Seconds() > 1 {
			ctx.Infof("Slow schedule: queue %s, gang cardinality %d, sample job id %s, time %fs", gctx.Queue, gctx.Cardinality(), gctx.JobIds()[0], duration.Seconds())
		}
		loopNumber++
	}
	ctx.Infof("optimiser completed %d loops in %s, scheduling %d jobs, preempting %d jobs",
		loopNumber, time.Now().Sub(start), len(scheduledJobs), len(preemptedJobs))
	return &SchedulerResult{
		ScheduledJobs: scheduledJobs,
		PreemptedJobs: preemptedJobs,
		NodeIdByJobId: nodeIdByJobId,
	}, nil
}

func hasContextExpired(ctx *armadacontext.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func isNearDeadline(ctx *armadacontext.Context) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return time.Until(deadline) < time.Millisecond*500
}

func (q *OptimisingQueueScheduler) createCandidateGangIterator(
	ctx *armadacontext.Context,
	sctx *schedulercontext.SchedulingContext,
) (CandidateGangIterator, error) {
	jobIteratorByQueue := make(map[string]JobContextIterator)
	for _, qctx := range sctx.QueueSchedulingContexts {
		// We only want to run on queues that are failing to achieve their fairshare
		// So skip any queue at or above its fairshare
		actualShare := sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx)
		if actualShare >= qctx.DemandCappedAdjustedFairShare {
			continue
		}
		queueIt := NewQueuedJobsIterator(ctx, qctx.Queue, sctx.Pool, q.jobDb)
		jobIteratorByQueue[qctx.Queue] = queueIt
	}

	gangIteratorsByQueue := make(map[string]*QueuedGangIterator, len(jobIteratorByQueue))
	for queue, it := range jobIteratorByQueue {
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, q.maxQueueLookBack, true)
	}

	candidateGangIterator, err := NewCostBasedCandidateGangIterator(sctx.Pool, sctx, sctx.FairnessCostProvider, gangIteratorsByQueue, false, q.prioritiseLargerJobs)
	if err != nil {
		return nil, err
	}
	return candidateGangIterator, nil
}

func (q *OptimisingQueueScheduler) checkIfWillBreachSchedulingLimits(
	gctx *schedulercontext.GangSchedulingContext,
	sctx *schedulercontext.SchedulingContext,
) (bool, string, error) {
	// TODO Return sctx back to original state somehow
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

	if gctx.RequestsFloatingResources {
		ok, unschedulableReason = q.floatingResourceTypes.WithinLimits(sctx.Pool, sctx.Allocated)
		if !ok {
			return ok, unschedulableReason, nil
		}
	}

	if _, err := sctx.EvictGang(gctx); err != nil {
		return false, "", err
	}

	return true, "", nil
}

func (q *OptimisingQueueScheduler) updateUnfeasibleSchedulingKeys(gctx *schedulercontext.GangSchedulingContext,
	sctx *schedulercontext.SchedulingContext, unschedulableReason string,
) {
	globallyUnschedulable := schedulerconstraints.UnschedulableReasonIsPropertyOfGang(unschedulableReason)

	// Register globally unfeasible scheduling keys.
	//
	// Only record unfeasible scheduling keys for single-job gangs.
	// Since a gang may be unschedulable even if all its members are individually schedulable.
	if gctx.Cardinality() == 1 && globallyUnschedulable {
		jctx := gctx.JobSchedulingContexts[0]
		schedulingKey, ok := jctx.SchedulingKey()
		if ok && schedulingKey != internaltypes.EmptySchedulingKey {
			if _, ok := sctx.UnfeasibleSchedulingKeys[schedulingKey]; !ok {
				// Keep the first jctx for each unfeasible schedulingKey.
				sctx.UnfeasibleSchedulingKeys[schedulingKey] = jctx
			}
		}
	}
}
