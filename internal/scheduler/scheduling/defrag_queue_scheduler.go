package scheduling

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type DefragQueueScheduler struct {
	nodeScheduler    *NodeScheduler
	jobDb            JobRepository
	maxQueueLookBack uint
}

func NewDefragQueueScheduler(jobDb JobRepository, nodeScheduler *NodeScheduler, maxQueueLookBack uint) *DefragQueueScheduler {
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
	nodeIdByJobId := make(map[string]string)

	for {
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
		scheduledOk, unschedulableReason, err := q.nodeScheduler.Schedule(ctx, gctx, sctx)
		if err != nil {
			return nil, err
		} else if scheduledOk {
			for _, jctx := range gctx.JobSchedulingContexts {
				if pctx := jctx.PodSchedulingContext; pctx.IsSuccessful() {
					scheduledJobs = append(scheduledJobs, jctx)
					nodeIdByJobId[jctx.JobId] = pctx.NodeId
				}
			}
		} else if schedulerconstraints.IsTerminalUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled,
			// instruct the underlying iterator to only yield evicted jobs from now on.
			sctx.TerminationReason = unschedulableReason
			gangIterator.OnlyYieldEvicted()
		} else if schedulerconstraints.IsTerminalQueueUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled for this queue,
			// instruct the underlying iterator to only yield evicted jobs for this queue from now on.
			gangIterator.OnlyYieldEvictedForQueue(gctx.Queue)
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
