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
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
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
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	nodeDb *nodedb.NodeDb,
	jobIteratorByQueue map[string]JobContextIterator,
	skipUnsuccessfulSchedulingKeyCheck bool,
	considerPriorityClassPriority bool,
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
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, constraints.GetMaxQueueLookBack(), true)
	}
	candidateGangIterator, err := NewCandidateGangIterator(sctx.Pool, sctx, sctx.FairnessCostProvider, gangIteratorsByQueue, considerPriorityClassPriority)
	if err != nil {
		return nil, err
	}
	return &QueueScheduler{
		schedulingContext:     sctx,
		candidateGangIterator: candidateGangIterator,
		gangScheduler:         gangScheduler,
	}, nil
}

func (sch *QueueScheduler) Schedule(ctx *armadacontext.Context) (*SchedulerResult, error) {
	var scheduledJobs []*schedulercontext.JobSchedulingContext

	nodeIdByJobId := make(map[string]string)
	ctx.Infof("Looping through candidate gangs for pool %s...", sch.schedulingContext.Pool)

	statsPerQueue := map[string]QueueStats{}
	loopNumber := 0
	for {
		// Peek() returns the next gang to try to schedule. Call Clear() before calling Peek() again.
		// Calling Clear() after (failing to) schedule ensures we get the next gang in order of smallest fair share.
		gctx, queueCostInclGang, err := sch.candidateGangIterator.Peek()
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
		} else if schedulerconstraints.IsTerminalUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled,
			// instruct the underlying iterator to only yield evicted jobs from now on.
			sch.schedulingContext.TerminationReason = unschedulableReason
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
			queue, queueOK := sch.candidateGangIterator.queueRepository.GetQueue(gctx.Queue)
			if queueOK {
				stats.LastGangScheduledResources = gctx.TotalResourceRequests.DeepCopy()
				stats.LastGangScheduledQueueResources = queue.GetAllocation().DeepCopy()
			} else {
				stats.LastGangScheduledResources = schedulerobjects.NewResourceListWithDefaultSize()
				stats.LastGangScheduledQueueResources = schedulerobjects.NewResourceListWithDefaultSize()
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

	ctx.Infof("Finished %d loops through candidate gangs for pool %s: details %v", loopNumber, sch.schedulingContext.Pool, armadamaps.MapValues(statsPerQueue, func(s QueueStats) string {
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
			s.LastGangScheduledResources.CompactString(),
			s.LastGangScheduledQueueResources.CompactString(),
			s.Time.Seconds())
	}))

	if sch.schedulingContext.TerminationReason == "" {
		sch.schedulingContext.TerminationReason = "no remaining candidate jobs"
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
		SchedulingContexts: []*schedulercontext.SchedulingContext{sch.schedulingContext},
		PerPoolSchedulingStats: map[string]PerPoolSchedulingStats{
			sch.schedulingContext.Pool: schedulingStats,
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

// CandidateGangIterator determines which gang to try scheduling next across queues.
// Specifically, it yields the next gang in the queue with smallest fraction of its fair share,
// where the fraction of fair share computation includes the yielded gang.
type CandidateGangIterator struct {
	pool                 string
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
	pool string,
	queueRepository fairness.QueueRepository,
	fairnessCostProvider fairness.FairnessCostProvider,
	iteratorsByQueue map[string]*QueuedGangIterator,
	considerPriority bool,
) (*CandidateGangIterator, error) {
	it := &CandidateGangIterator{
		pool:                    pool,
		queueRepository:         queueRepository,
		fairnessCostProvider:    fairnessCostProvider,
		onlyYieldEvictedByQueue: make(map[string]bool),
		buffer:                  schedulerobjects.NewResourceListWithDefaultSize(),
		pq: QueueCandidateGangIteratorPQ{
			considerPriority: considerPriority,
			items:            make([]*QueueCandidateGangIteratorItem, 0, len(iteratorsByQueue)),
		},
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

func (it *CandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, float64, error) {
	if it.pq.Len() == 0 {
		// No queued jobs left.
		return nil, 0.0, nil
	}
	first := it.pq.items[0]
	return first.gctx, first.queueCost, nil
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
	item.gctx = gctx
	cost, err := it.queueCostWithGctx(gctx)
	if err != nil {
		return err
	}
	item.queueCost = cost

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

// queueCostWithGctx returns the cost associated with a queue if gctx were to be scheduled.
func (it *CandidateGangIterator) queueCostWithGctx(gctx *schedulercontext.GangSchedulingContext) (float64, error) {
	gangQueue := gctx.Queue
	if len(gctx.JobSchedulingContexts) > 0 && !gctx.JobSchedulingContexts[0].IsHomeJob(it.pool) {
		gangQueue = schedulercontext.CalculateAwayQueueName(gctx.Queue)
	}
	queue, ok := it.queueRepository.GetQueue(gangQueue)
	if !ok {
		return 0, errors.Errorf("unknown queue %s", gangQueue)
	}
	it.buffer.Zero()
	it.buffer.Add(queue.GetAllocation())
	it.buffer.Add(gctx.TotalResourceRequests)
	return it.fairnessCostProvider.WeightedCostFromAllocation(it.buffer, queue.GetWeight()), nil
}

// QueueCandidateGangIteratorPQ is a priority queue used by CandidateGangIterator to determine from which queue to schedule the next job.
type QueueCandidateGangIteratorPQ struct {
	considerPriority bool
	items            []*QueueCandidateGangIteratorItem
}

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
	queueCost             float64
	priorityClassPriority int32
	// The index of the item in the heap.
	// maintained by the heap.Interface methods.
	index int
}

func (pq *QueueCandidateGangIteratorPQ) Len() int { return len(pq.items) }

func (pq *QueueCandidateGangIteratorPQ) Less(i, j int) bool {
	// Consider priority class priority first
	if pq.considerPriority && pq.items[i].priorityClassPriority != pq.items[j].priorityClassPriority {
		return pq.items[i].priorityClassPriority > pq.items[j].priorityClassPriority
	}

	// Then queue cost
	if pq.items[i].queueCost != pq.items[j].queueCost {
		return pq.items[i].queueCost < pq.items[j].queueCost
	}

	// Tie-break by queue name.
	return pq.items[i].queue < pq.items[j].queue
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
