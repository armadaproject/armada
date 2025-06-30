package scheduling

import (
	"container/heap"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
)

type CandidateGangIterator interface {
	Peek() (*schedulercontext.GangSchedulingContext, float64, error)
	Clear() error
	GetAllocationForQueue(queue string) (internaltypes.ResourceList, bool)
	OnlyYieldEvicted()
	OnlyYieldEvictedForQueue(queue string)
}

// QueueScheduler is responsible for choosing the order in which to attempt scheduling queued gangs.
// Relies on GangScheduler for scheduling once a gang is chosen.
type QueueScheduler struct {
	schedulingContext     *schedulercontext.SchedulingContext
	candidateGangIterator CandidateGangIterator
	gangScheduler         *GangScheduler
	marketDriven          bool
	spotPriceCutoff       float64
}

func NewQueueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	nodeDb *nodedb.NodeDb,
	jobIteratorByQueue map[string]JobContextIterator,
	skipUnsuccessfulSchedulingKeyCheck bool,
	considerPriorityClassPriority bool,
	prioritiseLargerJobs bool,
	maxQueueLookBack uint,
	marketDriven bool,
	spotPriceCutoff float64,
) (*QueueScheduler, error) {
	for queue := range jobIteratorByQueue {
		if _, ok := sctx.QueueSchedulingContexts[queue]; !ok {
			return nil, errors.Errorf("no scheduling context for queue %s", queue)
		}
	}
	gangScheduler, err := NewGangScheduler(sctx, constraints, floatingResourceTypes, nodeDb, skipUnsuccessfulSchedulingKeyCheck)
	if err != nil {
		return nil, err
	}
	gangIteratorsByQueue := make(map[string]*QueuedGangIterator)
	for queue, it := range jobIteratorByQueue {
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, maxQueueLookBack, true)
	}
	var candidateGangIterator CandidateGangIterator
	if marketDriven {
		candidateGangIterator, err = NewMarketCandidateGangIterator(sctx.Pool, sctx, gangIteratorsByQueue)
		if err != nil {
			return nil, err
		}
	} else {
		candidateGangIterator, err = NewCostBasedCandidateGangIterator(sctx.Pool, sctx, sctx.FairnessCostProvider, gangIteratorsByQueue, considerPriorityClassPriority, prioritiseLargerJobs)
		if err != nil {
			return nil, err
		}
	}
	return &QueueScheduler{
		schedulingContext:     sctx,
		candidateGangIterator: candidateGangIterator,
		gangScheduler:         gangScheduler,
		marketDriven:          marketDriven,
		spotPriceCutoff:       spotPriceCutoff,
	}, nil
}

func (sch *QueueScheduler) Schedule(ctx *armadacontext.Context) (*SchedulerResult, error) {
	var scheduledJobs []*schedulercontext.JobSchedulingContext
	sctx := sch.schedulingContext

	nodeIdByJobId := make(map[string]string)
	ctx.Infof("Looping through candidate gangs for pool %s...", sctx.Pool)

	statsPerQueue := map[string]QueueStats{}
	loopNumber := 0
	for {
		// Peek() returns the next gang to try to schedule. Call Clear() before calling Peek() again.
		// Calling Clear() after (failing to) schedule ensures we get the next gang in order of smallest fair share.
		gctx, queueCostInclGang, err := sch.candidateGangIterator.Peek()
		if err != nil {
			sctx.TerminationReason = err.Error()
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
			sctx.TerminationReason = err.Error()
			return nil, err
		default:
		}
		start := time.Now()
		scheduledOk, unschedulableReason, err := sch.gangScheduler.Schedule(ctx, gctx)
		if err != nil {
			return nil, err
		} else if scheduledOk {
			for _, jctx := range gctx.JobSchedulingContexts {
				if pctx := jctx.PodSchedulingContext; pctx.IsSuccessful() {
					scheduledJobs = append(scheduledJobs, jctx)
					nodeIdByJobId[jctx.JobId] = pctx.NodeId
				}
			}

			if sch.marketDriven && sctx.SpotPrice == nil {
				totalAllocation := sctx.FairnessCostProvider.UnweightedCostFromAllocation(sctx.Allocated)
				if totalAllocation > sch.spotPriceCutoff {
					price := gctx.JobSchedulingContexts[0].Job.GetBidPrice(sctx.Pool)
					for _, jctx := range gctx.JobSchedulingContexts {
						if jctx.Job.GetBidPrice(sctx.Pool) < price {
							price = jctx.Job.GetBidPrice(sctx.Pool)
						}
					}

					sctx.SpotPrice = &price
					for _, qctx := range sctx.QueueSchedulingContexts {
						qctx.BillableAllocation = qctx.Allocated
					}
				}
			}
		} else if schedulerconstraints.IsTerminalUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled,
			// instruct the underlying iterator to only yield evicted jobs from now on.
			sctx.TerminationReason = unschedulableReason
			sch.candidateGangIterator.OnlyYieldEvicted()
		} else if schedulerconstraints.IsTerminalQueueUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled for this queue,
			// instruct the underlying iterator to only yield evicted jobs for this queue from now on.
			sch.candidateGangIterator.OnlyYieldEvictedForQueue(gctx.Queue)
		}

		duration := time.Now().Sub(start)
		stats := statsPerQueue[gctx.Queue]

		stats.GangsConsidered++
		stats.JobsConsidered += gctx.Cardinality()
		if scheduledOk {
			stats.GangsScheduled++
		}

		if stats.FirstGangConsideredSampleJobId == "" {
			stats.FirstGangConsideredSampleJobId = gctx.JobIds()[0]
			stats.FirstGangConsideredQueuePosition = loopNumber
			if scheduledOk {
				stats.FirstGangConsideredResult = "scheduled"
			} else {
				stats.FirstGangConsideredResult = unschedulableReason
			}
		}

		if scheduledOk {
			stats.LastGangScheduledSampleJobId = gctx.JobIds()[0]
			stats.LastGangScheduledQueueCost = queueCostInclGang
			stats.LastGangScheduledQueuePosition = loopNumber
			allocation, ok := sch.candidateGangIterator.GetAllocationForQueue(gctx.Queue)
			if ok {
				stats.LastGangScheduledResources = gctx.TotalResourceRequests
				stats.LastGangScheduledQueueResources = allocation
			} else {
				stats.LastGangScheduledResources = internaltypes.ResourceList{}
				stats.LastGangScheduledQueueResources = internaltypes.ResourceList{}
			}
		}

		stats.Time += duration
		statsPerQueue[gctx.Queue] = stats
		if duration.Seconds() > 1 {
			ctx.Infof("Slow schedule: queue %s, gang cardinality %d, sample job id %s, time %fs", gctx.Queue, gctx.Cardinality(), gctx.JobIds()[0], duration.Seconds())
		}

		// Clear() to get the next gang in order of smallest fair share.
		// Calling clear here ensures the gang scheduled in this iteration is accounted for.
		if err := sch.candidateGangIterator.Clear(); err != nil {
			return nil, err
		}

		loopNumber++
	}

	ctx.Infof("Finished %d loops through candidate gangs for pool %s: details %v", loopNumber, sctx.Pool, armadamaps.MapValues(statsPerQueue, func(s QueueStats) string {
		return fmt.Sprintf("{gangsConsidered=%d, jobsConsidered=%d, gangsScheduled=%d, "+
			"firstGangConsideredSampleJobId=%s, firstGangConsideredResult=%s, firstGangConsideredQueuePosition=%d, "+
			"lastGangScheduledSampleJobId=%s, lastGangScheduledQueuePosition=%d, lastGangScheduledQueueCost=%f,"+
			"lastGangScheduledResources=%s, lastGangScheduledQueueResources=%s, time=%fs}",
			s.GangsConsidered,
			s.JobsConsidered,
			s.GangsScheduled,
			s.FirstGangConsideredSampleJobId,
			s.FirstGangConsideredResult,
			s.FirstGangConsideredQueuePosition,
			s.LastGangScheduledSampleJobId,
			s.LastGangScheduledQueuePosition,
			s.LastGangScheduledQueueCost,
			s.LastGangScheduledResources.String(),
			s.LastGangScheduledQueueResources.String(),
			s.Time.Seconds())
	}))

	if sctx.TerminationReason == "" {
		sctx.TerminationReason = "no remaining candidate jobs"
	}
	if len(scheduledJobs) != len(nodeIdByJobId) {
		return nil, errors.Errorf("only %d out of %d jobs mapped to a node", len(nodeIdByJobId), len(scheduledJobs))
	}

	schedulingStats := PerPoolSchedulingStats{
		StatsPerQueue: statsPerQueue,
		LoopNumber:    loopNumber,
	}

	return &SchedulerResult{
		PreemptedJobs:      nil,
		ScheduledJobs:      scheduledJobs,
		NodeIdByJobId:      nodeIdByJobId,
		SchedulingContexts: []*schedulercontext.SchedulingContext{sctx},
		PerPoolSchedulingStats: map[string]PerPoolSchedulingStats{
			sctx.Pool: schedulingStats,
		},
	}, nil
}

// QueuedGangIterator is an iterator over queued gangs.
// Each gang is yielded once its final member is received from the underlying iterator.
// Jobs without gangIdAnnotation are considered gangs of cardinality 1.
type QueuedGangIterator struct {
	schedulingContext  *schedulercontext.SchedulingContext
	queuedJobsIterator JobContextIterator
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

func NewQueuedGangIterator(sctx *schedulercontext.SchedulingContext, it JobContextIterator, maxLookback uint, skipKnownUnschedulableJobs bool) *QueuedGangIterator {
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
			if ok && schedulingKey != internaltypes.EmptySchedulingKey {
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
		if gangId := jctx.GangInfo.Id; gangId != "" {
			gang := it.jctxsByGangId[gangId]
			gang = append(gang, jctx)
			it.jctxsByGangId[gangId] = gang
			if len(gang) == jctx.GangInfo.Cardinality {
				delete(it.jctxsByGangId, gangId)
				it.next = schedulercontext.NewGangSchedulingContext(gang)
				return it.next, nil
			}
		} else {
			// It's not actually necessary to treat this case separately, but
			// using the empty string as a key in it.jctxsByGangId sounds like
			// it would get us in trouble later down the line.
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

// CostBasedCandidateGangIterator determines which gang to try scheduling next across queues.
// Specifically, it yields the next gang in the queue with smallest fraction of its fair share,
// where the fraction of fair share computation includes the yielded gang.
type CostBasedCandidateGangIterator struct {
	pool                 string
	queueRepository      fairness.QueueRepository
	fairnessCostProvider fairness.FairnessCostProvider
	// If true, this iterator only yields gangs where all jobs are evicted.
	onlyYieldEvicted bool
	// If, e.g., onlyYieldEvictedByQueue["A"] is true,
	// this iterator only yields gangs where all jobs are evicted for queue A.
	onlyYieldEvictedByQueue map[string]bool
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq QueueCandidateGangIteratorPQ
}

func (it *CostBasedCandidateGangIterator) GetAllocationForQueue(queue string) (internaltypes.ResourceList, bool) {
	q, ok := it.queueRepository.GetQueue(queue)
	if !ok {
		return internaltypes.ResourceList{}, false
	}
	return q.GetAllocation(), true
}

func NewCostBasedCandidateGangIterator(
	pool string,
	queueRepository fairness.QueueRepository,
	fairnessCostProvider fairness.FairnessCostProvider,
	iteratorsByQueue map[string]*QueuedGangIterator,
	considerPriority bool,
	prioritiseLargerJobs bool,
) (*CostBasedCandidateGangIterator, error) {
	it := &CostBasedCandidateGangIterator{
		pool:                    pool,
		queueRepository:         queueRepository,
		fairnessCostProvider:    fairnessCostProvider,
		onlyYieldEvictedByQueue: make(map[string]bool),
		pq: QueueCandidateGangIteratorPQ{
			considerPriority:     considerPriority,
			prioritiseLargerJobs: prioritiseLargerJobs,
			items:                make([]*QueueCandidateGangIteratorItem, 0, len(iteratorsByQueue)),
		},
	}
	for queue, queueIt := range iteratorsByQueue {
		qctx, present := queueIt.schedulingContext.QueueSchedulingContexts[queue]
		if !present {
			return nil, fmt.Errorf("unable to find queue context for queue %s", queue)
		}
		queueBudget := qctx.DemandCappedAdjustedFairShare / qctx.Weight
		if _, err := it.updateAndPushPQItem(it.newPQItem(queue, queueBudget, queueIt)); err != nil {
			return nil, err
		}
	}
	return it, nil
}

func (it *CostBasedCandidateGangIterator) OnlyYieldEvicted() {
	it.onlyYieldEvicted = true
}

func (it *CostBasedCandidateGangIterator) OnlyYieldEvictedForQueue(queue string) {
	it.onlyYieldEvictedByQueue[queue] = true
}

// Clear removes the first item in the iterator.
// If it.onlyYieldEvicted is true, any consecutive non-evicted jobs are also removed.
func (it *CostBasedCandidateGangIterator) Clear() error {
	if it.pq.Len() == 0 {
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
		for it.pq.Len() > 0 && !it.pq.items[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
	} else {
		// Same check as above on a per-queue basis.
		for it.pq.Len() > 0 && it.onlyYieldEvictedByQueue[it.pq.items[0].gctx.Queue] && !it.pq.items[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
	}
	return nil
}

func (it *CostBasedCandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, float64, error) {
	if it.pq.Len() == 0 {
		// No queued jobs left.
		return nil, 0.0, nil
	}
	first := it.pq.items[0]
	return first.gctx, first.proposedQueueCost, nil
}

func (it *CostBasedCandidateGangIterator) newPQItem(queue string, queueBudget float64, queueIt *QueuedGangIterator) *QueueCandidateGangIteratorItem {
	return &QueueCandidateGangIteratorItem{
		queue:       queue,
		queueBudget: queueBudget,
		it:          queueIt,
	}
}

func (it *CostBasedCandidateGangIterator) updateAndPushPQItem(item *QueueCandidateGangIteratorItem) (bool, error) {
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

func (it *CostBasedCandidateGangIterator) updatePQItem(item *QueueCandidateGangIteratorItem) error {
	item.gctx = nil
	item.proposedQueueCost = 0
	item.currentQueueCost = 0
	item.itemSize = 0
	gctx, err := item.it.Peek()
	if err != nil {
		return err
	}
	if gctx == nil {
		return nil
	}
	item.gctx = gctx
	queue, err := it.getQueue(gctx)
	if err != nil {
		return err
	}
	item.proposedQueueCost = it.fairnessCostProvider.WeightedCostFromAllocation(queue.GetAllocationInclShortJobPenalty().Add(gctx.TotalResourceRequests), queue.GetWeight())
	item.currentQueueCost = it.fairnessCostProvider.WeightedCostFromAllocation(queue.GetAllocationInclShortJobPenalty(), queue.GetWeight())
	// We multiply here, as queue weights are a fraction
	// So for the same job size, highly weighted queues jobs will look larger
	item.itemSize = it.fairnessCostProvider.UnweightedCostFromAllocation(gctx.TotalResourceRequests) * queue.GetWeight()

	// The PQItem needs to have a priority class priority for the whole gang.  This may not be uniform as different
	// Gang members may have been scheduled at different priorities due to home/away preemption. We therefore take the
	// lowest priority across the whole gang
	item.priorityClassPriority = math.MaxInt32
	for _, jobCtx := range gctx.JobSchedulingContexts {
		newPriority := jobCtx.Job.PriorityClass().Priority
		if jobCtx.PodSchedulingContext != nil { // Jobs was already scheduled in this cycle, us the priority from that
			newPriority = jobCtx.PodSchedulingContext.ScheduledAtPriority
		} else {
			priority, ok := jobCtx.Job.ScheduledAtPriority()
			if ok { // Job was scheduled in a previous cycle
				newPriority = priority
			}
		}

		if newPriority < item.priorityClassPriority {
			item.priorityClassPriority = newPriority
		}
	}

	return nil
}

// returns the queue of the supplied gctx
func (it *CostBasedCandidateGangIterator) getQueue(gctx *schedulercontext.GangSchedulingContext) (fairness.Queue, error) {
	gangQueue := gctx.Queue
	if len(gctx.JobSchedulingContexts) > 0 && !gctx.JobSchedulingContexts[0].IsHomeJob(it.pool) {
		gangQueue = schedulercontext.CalculateAwayQueueName(gctx.Queue)
	}
	queue, ok := it.queueRepository.GetQueue(gangQueue)
	if !ok {
		return nil, errors.Errorf("unknown queue %s", gangQueue)
	}
	return queue, nil
}

// QueueCandidateGangIteratorPQ is a priority queue used by CandidateGangIterator to determine from which queue to schedule the next job.
type QueueCandidateGangIteratorPQ struct {
	considerPriority     bool
	prioritiseLargerJobs bool
	items                []*QueueCandidateGangIteratorItem
}

type QueueCandidateGangIteratorItem struct {
	// Each item corresponds to a queue.
	queue string
	// Iterator for this queue.
	it *QueuedGangIterator
	// Most recent value produced by the iterator.
	// Cached here to avoid repeating scheduling checks unnecessarily.
	gctx *schedulercontext.GangSchedulingContext
	// Cost associated with the queue if the top most gang in the queue were to be scheduled.
	proposedQueueCost float64
	// Current cost associated with the queue
	currentQueueCost float64
	// The cost allocated to the queue based on fairshare
	// used to compare with proposedQueueCost to determine if scheduling the next item will put the queue over its fairshare
	queueBudget float64
	// The size of top most gang
	// Used to determine which job is larger
	itemSize              float64
	priorityClassPriority int32
	// The index of the item in the heap.
	// maintained by the heap.Interface methods.
	index int
}

func (pq *QueueCandidateGangIteratorPQ) Len() int { return len(pq.items) }

func (pq *QueueCandidateGangIteratorPQ) Less(i, j int) bool {
	item1 := pq.items[i]
	item2 := pq.items[j]

	// Consider priority class priority first
	if pq.considerPriority && item1.priorityClassPriority != item2.priorityClassPriority {
		return item1.priorityClassPriority > item2.priorityClassPriority
	}

	if pq.prioritiseLargerJobs {
		if item1.proposedQueueCost <= item1.queueBudget && item2.proposedQueueCost <= item2.queueBudget {
			// If adding the items results in neither queue exceeding its fairshare
			// Take the largest job if the queues are equal current cost (which is the case if all jobs get evicted / on an empty farm)
			// The reason we prefer larger jobs is:
			// - It reduces fragmentation - a typical strategy is to schedule larger jobs first as smaller jobs can fit in around larger jobs
			// - It makes it easier for larger jobs to get on and helps to reduce to bias towards smaller jobs.
			//   Particularly helpful if users have a single large gang they want to get on, as they'll get considered first
			if item1.currentQueueCost == item2.currentQueueCost && item1.itemSize != item2.itemSize {
				return item1.itemSize > item2.itemSize
			}
			// Otherwise let whichever queue has the lowest current cost go first, regardless of job size
			// This is so that:
			// - We interleave smaller jobs and don't just schedule a queue of large jobs first until it hits its fairshare
			// - So we don't block queues with larger jobs from getting on as they make a bigger step than queues with smaller jobs
			if item1.currentQueueCost != item2.currentQueueCost {
				return item1.currentQueueCost < item2.currentQueueCost
			}
		} else if item1.proposedQueueCost > item1.queueBudget && item2.proposedQueueCost > item2.queueBudget {
			// If adding the items results in both queues being above their fairshare
			//  take the item that results in the smallest amount over the fairshare
			if item1.proposedQueueCost != item2.proposedQueueCost {
				return item1.proposedQueueCost < item2.proposedQueueCost
			}
		} else if item1.proposedQueueCost <= item1.queueBudget {
			return true
		} else if item2.proposedQueueCost <= item2.queueBudget {
			return false
		}
	} else {
		if item1.proposedQueueCost != item2.proposedQueueCost {
			return item1.proposedQueueCost < item2.proposedQueueCost
		}
	}

	// Tie-break by queue name.
	return item1.queue < item2.queue
}

func (pq *QueueCandidateGangIteratorPQ) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *QueueCandidateGangIteratorPQ) Push(x any) {
	n := pq.Len()
	item := x.(*QueueCandidateGangIteratorItem)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *QueueCandidateGangIteratorPQ) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}
