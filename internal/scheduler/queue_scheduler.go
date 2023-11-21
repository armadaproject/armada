package scheduler

import (
	"container/heap"
	"reflect"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// QueueScheduler is responsible for choosing the order in which to attempt scheduling queued gangs.
// Relies on GangScheduler for scheduling once a gang is chosen.
type QueueScheduler struct {
	schedulingContext     *schedulercontext.SchedulingContext
	candidateGangIterator *CandidateGangIterator
	gangScheduler         *GangScheduler
}

func NewQueueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	nodeDb *nodedb.NodeDb,
	jobIteratorByQueue map[string]JobIterator,
) (*QueueScheduler, error) {
	for queue := range jobIteratorByQueue {
		if _, ok := sctx.QueueSchedulingContexts[queue]; !ok {
			return nil, errors.Errorf("no scheduling context for queue %s", queue)
		}
	}
	gangScheduler, err := NewGangScheduler(sctx, constraints, nodeDb)
	if err != nil {
		return nil, err
	}
	gangIteratorsByQueue := make(map[string]*QueuedGangIterator)
	for queue, it := range jobIteratorByQueue {
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, constraints.MaxQueueLookback, true)
	}
	candidateGangIterator, err := NewCandidateGangIterator(sctx, sctx.FairnessCostProvider, gangIteratorsByQueue)
	if err != nil {
		return nil, err
	}
	return &QueueScheduler{
		schedulingContext:     sctx,
		candidateGangIterator: candidateGangIterator,
		gangScheduler:         gangScheduler,
	}, nil
}

func (sch *QueueScheduler) SkipUnsuccessfulSchedulingKeyCheck() {
	sch.gangScheduler.SkipUnsuccessfulSchedulingKeyCheck()
}

func (sch *QueueScheduler) Schedule(ctx *armadacontext.Context) (*schedulerobjects.SchedulerResult, error) {
	nodeIdByJobId := make(map[string]string)
	scheduledJobs := make([]interfaces.LegacySchedulerJob, 0)
	failedJobs := make([]interfaces.LegacySchedulerJob, 0)
	for {
		// Peek() returns the next gang to try to schedule. Call Clear() before calling Peek() again.
		// Calling Clear() after (failing to) schedule ensures we get the next gang in order of smallest fair share.
		gctx, err := sch.candidateGangIterator.Peek()
		if err != nil {
			sch.schedulingContext.TerminationReason = err.Error()
			return nil, err
		}
		if gctx == nil {
			break
		}
		if gctx.Cardinality() == 0 {
			if err := sch.candidateGangIterator.Clear(); err != nil {
				return nil, err
			}
			continue
		}
		select {
		case <-ctx.Done():
			// TODO: Better to push ctx into next and have that control it.
			err := ctx.Err()
			sch.schedulingContext.TerminationReason = err.Error()
			return nil, err
		default:
		}
		if ok, unschedulableReason, err := sch.gangScheduler.Schedule(ctx, gctx); err != nil {
			return nil, err
		} else if ok {
			// We scheduled the minimum number of gang jobs required.
			for _, jctx := range gctx.JobSchedulingContexts {
				pctx := jctx.PodSchedulingContext
				if pctx != nil && pctx.NodeId != "" {
					scheduledJobs = append(scheduledJobs, jctx.Job)
					nodeIdByJobId[jctx.JobId] = pctx.NodeId
				}
			}

			// Report any excess gang jobs that failed
			for _, jctx := range gctx.JobSchedulingContexts {
				if jctx.ShouldFail {
					failedJobs = append(failedJobs, jctx.Job)
				}
			}
		} else if schedulerconstraints.IsTerminalUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled,
			// instruct the underlying iterator to only yield evicted jobs from now on.
			sch.candidateGangIterator.OnlyYieldEvicted()
		} else if schedulerconstraints.IsTerminalQueueUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled for this queue,
			// instruct the underlying iterator to only yield evicted jobs for this queue from now on.
			sch.candidateGangIterator.OnlyYieldEvictedForQueue(gctx.Queue)
		}

		// Clear() to get the next gang in order of smallest fair share.
		// Calling clear here ensures the gang scheduled in this iteration is accounted for.
		if err := sch.candidateGangIterator.Clear(); err != nil {
			return nil, err
		}
	}
	if sch.schedulingContext.TerminationReason == "" {
		sch.schedulingContext.TerminationReason = "no remaining candidate jobs"
	}
	if len(scheduledJobs) != len(nodeIdByJobId) {
		return nil, errors.Errorf("only %d out of %d jobs mapped to a node", len(nodeIdByJobId), len(scheduledJobs))
	}
	return &schedulerobjects.SchedulerResult{
		PreemptedJobs:      nil,
		ScheduledJobs:      scheduledJobs,
		FailedJobs:         failedJobs,
		NodeIdByJobId:      nodeIdByJobId,
		SchedulingContexts: []*schedulercontext.SchedulingContext{sch.schedulingContext},
	}, nil
}

// QueuedGangIterator is an iterator over queued gangs.
// Each gang is yielded once its final member is received from the underlying iterator.
// Jobs without gangIdAnnotation are considered gangs of cardinality 1.
type QueuedGangIterator struct {
	schedulingContext  *schedulercontext.SchedulingContext
	queuedJobsIterator JobIterator
	// Groups jctxs by the gang they belong to.
	jctxsByGangId map[string][]*schedulercontext.JobSchedulingContext
	// Maximum number of jobs to look at before giving up.
	maxLookback uint
	// If true, do not yield jobs known to be unschedulable.
	skipKnownUnschedulableJobs bool
	// Number of jobs we have seen so far.
	jobsSeen uint
	next     *schedulercontext.GangSchedulingContext
}

func NewQueuedGangIterator(sctx *schedulercontext.SchedulingContext, it JobIterator, maxLookback uint, skipKnownUnschedulableJobs bool) *QueuedGangIterator {
	return &QueuedGangIterator{
		schedulingContext:          sctx,
		queuedJobsIterator:         it,
		maxLookback:                maxLookback,
		skipKnownUnschedulableJobs: skipKnownUnschedulableJobs,
		jctxsByGangId:              make(map[string][]*schedulercontext.JobSchedulingContext),
	}
}

func (it *QueuedGangIterator) Next() (*schedulercontext.GangSchedulingContext, error) {
	if gctx, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return gctx, nil
	}
}

func (it *QueuedGangIterator) Clear() error {
	it.next = nil
	return nil
}

func (it *QueuedGangIterator) Peek() (*schedulercontext.GangSchedulingContext, error) {
	if it.hitLookbackLimit() {
		return nil, nil
	}
	if it.next != nil {
		return it.next, nil
	}

	// Get one job at a time from the underlying iterator until we either
	// 1. get a job that isn't part of a gang, in which case we yield it immediately, or
	// 2. get the final job in a gang, in which case we yield the entire gang.
	for {
		jctx, err := it.queuedJobsIterator.Next()
		if err != nil {
			return nil, err
		} else if jctx == nil || reflect.ValueOf(jctx).IsNil() {
			return nil, nil
		}

		// Queue lookback limits. Rescheduled jobs don't count towards the limit.
		if !jctx.IsEvicted {
			it.jobsSeen++
		}
		if it.hitLookbackLimit() {
			return nil, nil
		}

		// Skip this job if it's known to be unschedulable.
		if it.skipKnownUnschedulableJobs && len(it.schedulingContext.UnfeasibleSchedulingKeys) > 0 {
			schedulingKey, ok := jctx.SchedulingKey()
			if ok && schedulingKey != schedulerobjects.EmptySchedulingKey {
				if unsuccessfulJctx, ok := it.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey]; ok {
					// Since jctx would fail to schedule for the same reason as unsuccessfulJctx,
					// set the unschedulable reason and pctx equal to that of unsuccessfulJctx.
					jctx.UnschedulableReason = unsuccessfulJctx.UnschedulableReason
					jctx.PodSchedulingContext = unsuccessfulJctx.PodSchedulingContext
					if _, err := it.schedulingContext.AddJobSchedulingContext(jctx); err != nil {
						return nil, err
					}
					continue
				}
			}
		}
		if jctx.GangCardinality > 1 {
			gang := it.jctxsByGangId[jctx.GangId]
			gang = append(gang, jctx)
			it.jctxsByGangId[jctx.GangId] = gang
			if len(gang) == jctx.GangCardinality {
				delete(it.jctxsByGangId, jctx.GangId)
				it.next = schedulercontext.NewGangSchedulingContext(gang)
				return it.next, nil
			}
		} else {
			it.next = schedulercontext.NewGangSchedulingContext([]*schedulercontext.JobSchedulingContext{jctx})
			return it.next, nil
		}
	}
}

func (it *QueuedGangIterator) hitLookbackLimit() bool {
	if it.maxLookback == 0 {
		return false
	}
	return it.jobsSeen > it.maxLookback
}

// CandidateGangIterator determines which gang to try scheduling next across queues.
// Specifically, it yields the next gang in the queue with smallest fraction of its fair share,
// where the fraction of fair share computation includes the yielded gang.
type CandidateGangIterator struct {
	queueRepository      fairness.QueueRepository
	fairnessCostProvider fairness.FairnessCostProvider
	// If true, this iterator only yields gangs where all jobs are evicted.
	onlyYieldEvicted bool
	// If, e.g., onlyYieldEvictedByQueue["A"] is true,
	// this iterator only yields gangs where all jobs are evicted for queue A.
	onlyYieldEvictedByQueue map[string]bool
	// Reusable buffer to avoid allocations.
	buffer schedulerobjects.ResourceList
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq QueueCandidateGangIteratorPQ
}

func NewCandidateGangIterator(
	queueRepository fairness.QueueRepository,
	fairnessCostProvider fairness.FairnessCostProvider,
	iteratorsByQueue map[string]*QueuedGangIterator,
) (*CandidateGangIterator, error) {
	it := &CandidateGangIterator{
		queueRepository:         queueRepository,
		fairnessCostProvider:    fairnessCostProvider,
		onlyYieldEvictedByQueue: make(map[string]bool),
		buffer:                  schedulerobjects.NewResourceListWithDefaultSize(),
		pq:                      make(QueueCandidateGangIteratorPQ, 0, len(iteratorsByQueue)),
	}
	for queue, queueIt := range iteratorsByQueue {
		if _, err := it.updateAndPushPQItem(it.newPQItem(queue, queueIt)); err != nil {
			return nil, err
		}
	}
	return it, nil
}

func (it *CandidateGangIterator) OnlyYieldEvicted() {
	it.onlyYieldEvicted = true
}

func (it *CandidateGangIterator) OnlyYieldEvictedForQueue(queue string) {
	it.onlyYieldEvictedByQueue[queue] = true
}

// Clear removes the first item in the iterator.
// If it.onlyYieldEvicted is true, any consecutive non-evicted jobs are also removed.
func (it *CandidateGangIterator) Clear() error {
	if len(it.pq) == 0 {
		return nil
	}
	item := heap.Pop(&it.pq).(*QueueCandidateGangIteratorItem)
	if err := item.it.Clear(); err != nil {
		return err
	}
	if _, err := it.updateAndPushPQItem(item); err != nil {
		return err
	}

	// If set to only yield evicted gangs, drop any queues for which the next gang is non-evicted here.
	// We assume here that all evicted jobs appear before non-evicted jobs in the queue.
	// Hence, it's safe to drop a queue if the first job is non-evicted.
	if it.onlyYieldEvicted {
		for len(it.pq) > 0 && !it.pq[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
	} else {
		// Same check as above on a per-queue basis.
		for len(it.pq) > 0 && it.onlyYieldEvictedByQueue[it.pq[0].gctx.Queue] && !it.pq[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
	}
	return nil
}

func (it *CandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, error) {
	if len(it.pq) == 0 {
		// No queued jobs left.
		return nil, nil
	}
	return it.pq[0].gctx, nil
}

func (it *CandidateGangIterator) newPQItem(queue string, queueIt *QueuedGangIterator) *QueueCandidateGangIteratorItem {
	return &QueueCandidateGangIteratorItem{
		queue: queue,
		it:    queueIt,
	}
}

func (it *CandidateGangIterator) updateAndPushPQItem(item *QueueCandidateGangIteratorItem) (bool, error) {
	if err := it.updatePQItem(item); err != nil {
		return false, err
	}
	if item.gctx == nil {
		return false, nil
	}
	if it.onlyYieldEvicted && !item.gctx.AllJobsEvicted {
		return false, nil
	}
	if it.onlyYieldEvictedByQueue[item.gctx.Queue] && !item.gctx.AllJobsEvicted {
		return false, nil
	}
	heap.Push(&it.pq, item)
	return true, nil
}

func (it *CandidateGangIterator) updatePQItem(item *QueueCandidateGangIteratorItem) error {
	item.gctx = nil
	item.queueCost = 0
	gctx, err := item.it.Peek()
	if err != nil {
		return err
	}
	if gctx == nil {
		return nil
	}
	if gctx.Queue != item.queue {
		return errors.Errorf("mismatched queue %s and %s for gctx", gctx.Queue, item.queue)
	}
	item.gctx = gctx
	cost, err := it.queueCostWithGctx(gctx)
	if err != nil {
		return err
	}
	item.queueCost = cost
	return nil
}

// queueCostWithGctx returns the cost associated with a queue if gctx were to be scheduled.
func (it *CandidateGangIterator) queueCostWithGctx(gctx *schedulercontext.GangSchedulingContext) (float64, error) {
	queue, ok := it.queueRepository.GetQueue(gctx.Queue)
	if !ok {
		return 0, errors.Errorf("unknown queue %s", gctx.Queue)
	}
	it.buffer.Zero()
	it.buffer.Add(queue.GetAllocation())
	it.buffer.Add(gctx.TotalResourceRequests)
	return it.fairnessCostProvider.CostFromAllocationAndWeight(it.buffer, queue.GetWeight()), nil
}

// Priority queue used by CandidateGangIterator to determine from which queue to schedule the next job.
type QueueCandidateGangIteratorPQ []*QueueCandidateGangIteratorItem

type QueueCandidateGangIteratorItem struct {
	// Each item corresponds to a queue.
	queue string
	// Iterator for this queue.
	it *QueuedGangIterator
	// Most recent value produced by the iterator.
	// Cached here to avoid repeating scheduling checks unnecessarily.
	gctx *schedulercontext.GangSchedulingContext
	// Cost associated with the queue if the topmost gang in the queue were to be scheduled.
	// Used to order queues fairly.
	queueCost float64
	// The index of the item in the heap.
	// maintained by the heap.Interface methods.
	index int
}

func (pq QueueCandidateGangIteratorPQ) Len() int { return len(pq) }

func (pq QueueCandidateGangIteratorPQ) Less(i, j int) bool {
	// Tie-break by queue name.
	if pq[i].queueCost == pq[j].queueCost {
		return pq[i].queue < pq[j].queue
	}
	return pq[i].queueCost < pq[j].queueCost
}

func (pq QueueCandidateGangIteratorPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *QueueCandidateGangIteratorPQ) Push(x any) {
	n := len(*pq)
	item := x.(*QueueCandidateGangIteratorItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *QueueCandidateGangIteratorPQ) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
