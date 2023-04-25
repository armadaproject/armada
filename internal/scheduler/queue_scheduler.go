package scheduler

import (
	"container/heap"
	"context"
	"math"
	"reflect"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/logging"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
)

// If a job fails to schedule for one of these reasons, stop trying more jobs.
var terminalUnschedulableReasons = []string{
	schedulerconstraints.UnschedulableReasonMaximumNumberOfJobsScheduled,
	schedulerconstraints.UnschedulableReasonMaximumNumberOfGangsScheduled,
}

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
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, constraints.MaxLookbackPerQueue)
	}
	candidateGangIterator, err := NewCandidateGangIterator(sctx, gangIteratorsByQueue)
	if err != nil {
		return nil, err
	}
	return &QueueScheduler{
		schedulingContext:     sctx,
		candidateGangIterator: candidateGangIterator,
		gangScheduler:         gangScheduler,
	}, nil
}

func (sch *QueueScheduler) Schedule(ctx context.Context) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	if ResourceListAsWeightedApproximateFloat64(sch.schedulingContext.ResourceScarcity, sch.schedulingContext.TotalResources) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		log.Infof(
			"no resources with non-zero weight available for scheduling on any cluster: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.schedulingContext.TotalResources,
		)
		return &SchedulerResult{}, nil
	}
	if ResourceListAsWeightedApproximateFloat64(sch.schedulingContext.ResourceScarcity, sch.gangScheduler.nodeDb.TotalResources()) == 0 {
		// This refers to the resources currently considered for schedling.
		log.Infof(
			"no resources with non-zero weight available for scheduling in NodeDb: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.gangScheduler.nodeDb.TotalResources(),
		)
		return &SchedulerResult{}, nil
	}
	nodeIdByJobId := make(map[string]string)
	scheduledJobs := make([]interfaces.LegacySchedulerJob, 0)
	for {
		gctx, err := sch.candidateGangIterator.Next()
		if err != nil {
			sch.schedulingContext.TerminationReason = err.Error()
			return nil, err
		}
		if gctx == nil {
			break
		}
		if len(gctx.JobSchedulingContexts) == 0 {
			continue
		}
		select {
		case <-ctx.Done():
			// TODO: Better to push ctx into next and have that control it.
			sch.schedulingContext.TerminationReason = ctx.Err().Error()
			return nil, err
		default:
		}
		if ok, unschedulableReason, err := sch.gangScheduler.Schedule(ctx, gctx); err != nil {
			return nil, err
		} else if ok {
			for _, jctx := range gctx.JobSchedulingContexts {
				scheduledJobs = append(scheduledJobs, jctx.Job)
				if jctx.PodSchedulingContext != nil && jctx.PodSchedulingContext.Node != nil {
					nodeIdByJobId[jctx.JobId] = jctx.PodSchedulingContext.Node.Id
				}
			}
		} else {
			unschedulableReasonIsTerminal := false
			for _, reason := range terminalUnschedulableReasons {
				if unschedulableReason == reason {
					unschedulableReasonIsTerminal = true
					sch.schedulingContext.TerminationReason = reason
					break
				}
			}
			if unschedulableReasonIsTerminal {
				break
			}
		}
	}
	if sch.schedulingContext.TerminationReason == "" {
		sch.schedulingContext.TerminationReason = "no remaining candidate jobs"
	}
	if len(scheduledJobs) != len(nodeIdByJobId) {
		return nil, errors.Errorf("only %d out of %d jobs mapped to a node", len(nodeIdByJobId), len(scheduledJobs))
	}
	return &SchedulerResult{
		PreemptedJobs: nil,
		ScheduledJobs: scheduledJobs,
		NodeIdByJobId: nodeIdByJobId,
	}, nil
}

// QueuedGangIterator is an iterator over queued gangs.
// Each gang is yielded once its final member is received from the underlying iterator.
// Jobs without gangIdAnnotation are considered gangs of cardinality 1.
type QueuedGangIterator struct {
	schedulingContext  *schedulercontext.SchedulingContext
	queuedJobsIterator JobIterator
	// Groups jobs by the gang they belong to.
	jobsByGangId map[string][]interfaces.LegacySchedulerJob
	// Maximum number of jobs to look at before giving up.
	maxLookback uint
	// Number of jobs we have seen so far.
	jobsSeen uint
	next     *schedulercontext.GangSchedulingContext
}

func NewQueuedGangIterator(sctx *schedulercontext.SchedulingContext, it JobIterator, maxLookback uint) *QueuedGangIterator {
	return &QueuedGangIterator{
		schedulingContext:  sctx,
		queuedJobsIterator: it,
		maxLookback:        maxLookback,
		jobsByGangId:       make(map[string][]interfaces.LegacySchedulerJob),
	}
}

func (it *QueuedGangIterator) Next() (*schedulercontext.GangSchedulingContext, error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
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
		job, err := it.queuedJobsIterator.Next()
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, nil
		}
		if reflect.ValueOf(job).IsNil() {
			return nil, nil
		}
		// Rescheduled jobs don't count towards the limit.
		if !isEvictedJob(job) {
			it.jobsSeen++
		}
		if it.hitLookbackLimit() {
			return nil, nil
		}
		gangId, gangCardinality, isGangJob, err := GangIdAndCardinalityFromAnnotations(
			job.GetAnnotations(),
		)
		if err != nil {
			// TODO: Get from context passed in.
			log := logrus.NewEntry(logrus.New())
			logging.WithStacktrace(log, err).Errorf("failed to get gang cardinality for job %s", job.GetId())
			gangCardinality = 1 // Schedule jobs with invalid gang cardinality one by one.
		}
		if isGangJob {
			it.jobsByGangId[gangId] = append(it.jobsByGangId[gangId], job)
			gang := it.jobsByGangId[gangId]
			if len(gang) == gangCardinality {
				delete(it.jobsByGangId, gangId)
				it.next = schedulercontext.NewGangSchedulingContext(
					jobSchedulingContextsFromJobs(
						gang,
						it.schedulingContext.ExecutorId,
						it.schedulingContext.PriorityClasses,
					),
				)
				return it.next, nil
			}
		} else {
			it.next = schedulercontext.NewGangSchedulingContext(
				jobSchedulingContextsFromJobs(
					[]interfaces.LegacySchedulerJob{job},
					it.schedulingContext.ExecutorId,
					it.schedulingContext.PriorityClasses,
				),
			)
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
	SchedulingContext *schedulercontext.SchedulingContext
	// For each queue, weight is the inverse of the priority factor.
	weightByQueue map[string]float64
	// Sum of all weights.
	weightSum float64
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq QueueCandidateGangIteratorPQ
}

func NewCandidateGangIterator(
	sctx *schedulercontext.SchedulingContext,
	iteratorsByQueue map[string]*QueuedGangIterator,
) (*CandidateGangIterator, error) {
	weightSum := 0.0
	weightByQueue := make(map[string]float64)
	for queue := range iteratorsByQueue {
		qctx := sctx.QueueSchedulingContexts[queue]
		if qctx == nil {
			return nil, errors.Errorf("no scheduling context for queue %s", queue)
		}
		weight := 1 / math.Max(qctx.PriorityFactor, 1)
		weightByQueue[queue] = weight
		weightSum += weight
	}
	rv := &CandidateGangIterator{
		SchedulingContext: sctx,
		weightByQueue:     weightByQueue,
		weightSum:         weightSum,
		pq:                make(QueueCandidateGangIteratorPQ, 0, len(iteratorsByQueue)),
	}
	for queue, queueIt := range iteratorsByQueue {
		if err := rv.pushToPQ(queue, queueIt); err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func (it *CandidateGangIterator) pushToPQ(queue string, queueIt *QueuedGangIterator) error {
	gctx, err := queueIt.Peek()
	if err != nil {
		return err
	}
	if gctx == nil {
		return nil
	}
	totalResourcesForQueue := it.SchedulingContext.QueueSchedulingContexts[queue].ResourcesByPriority
	totalResourcesForQueueWithGang := totalResourcesForQueue.AggregateByResource()
	totalResourcesForQueueWithGang.Add(gctx.TotalResourceRequests)
	fairShare := it.weightByQueue[queue] / it.weightSum
	used := ResourceListAsWeightedApproximateFloat64(it.SchedulingContext.ResourceScarcity, totalResourcesForQueueWithGang)
	total := math.Max(ResourceListAsWeightedApproximateFloat64(it.SchedulingContext.ResourceScarcity, it.SchedulingContext.TotalResources), 1)
	fractionOfFairShare := (used / total) / fairShare
	item := &QueueCandidateGangIteratorItem{
		queue:               queue,
		it:                  queueIt,
		v:                   gctx,
		fractionOfFairShare: fractionOfFairShare,
	}
	heap.Push(&it.pq, item)
	return nil
}

func (it *CandidateGangIterator) Next() (*schedulercontext.GangSchedulingContext, error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func (it *CandidateGangIterator) Clear() error {
	if len(it.pq) == 0 {
		return nil
	}
	item := heap.Pop(&it.pq).(*QueueCandidateGangIteratorItem)
	if err := item.it.Clear(); err != nil {
		return err
	}
	if err := it.pushToPQ(item.queue, item.it); err != nil {
		return err
	}
	return nil
}

func (it *CandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, error) {
	// Yield a gang.
	// To ensure the last scheduled gang is accounted for,
	// pop and push items from/to the pq until we've seen the same queue twice consecutively,
	// since at that point we're sure pq priority for that item is correct.
	activeQueue := ""
	for {
		if len(it.pq) == 0 {
			// No queued jobs left.
			return nil, nil
		}
		item := heap.Pop(&it.pq).(*QueueCandidateGangIteratorItem)
		if item.queue != activeQueue {
			activeQueue = item.queue
			if err := it.pushToPQ(item.queue, item.it); err != nil {
				return nil, err
			}
			continue
		}
		gctx := item.v // Cached value is guaranteed to be fresh here.
		if err := it.pushToPQ(item.queue, item.it); err != nil {
			return nil, err
		}
		return gctx, nil
	}
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
	v *schedulercontext.GangSchedulingContext
	// Fraction of its fair share this queue would have
	// if its next schedulable job were to be scheduled.
	fractionOfFairShare float64
	// The index of the item in the heap.
	// maintained by the heap.Interface methods.
	index int
}

func (pq QueueCandidateGangIteratorPQ) Len() int { return len(pq) }

func (pq QueueCandidateGangIteratorPQ) Less(i, j int) bool {
	// Tie-break by queue name.
	if pq[i].fractionOfFairShare == pq[j].fractionOfFairShare {
		return pq[i].queue < pq[j].queue
	}
	return pq[i].fractionOfFairShare < pq[j].fractionOfFairShare
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
