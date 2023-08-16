package scheduler

import (
	"container/heap"
	"context"
	"reflect"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/logging"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
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
		gangIteratorsByQueue[queue] = NewQueuedGangIterator(sctx, it, constraints.MaxQueueLookback)
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

func (sch *QueueScheduler) SkipUnsuccessfulSchedulingKeyCheck() {
	sch.gangScheduler.SkipUnsuccessfulSchedulingKeyCheck()
}

func (sch *QueueScheduler) Schedule(ctx context.Context) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	if sch.schedulingContext.TotalResources.AsWeightedMillis(sch.schedulingContext.ResourceScarcity) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		log.Infof(
			"no resources with non-zero weight available for scheduling on any cluster: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.schedulingContext.TotalResources,
		)
		return &SchedulerResult{}, nil
	}
	if rl := sch.gangScheduler.nodeDb.TotalResources(); rl.AsWeightedMillis(sch.schedulingContext.ResourceScarcity) == 0 {
		// This refers to the resources currently considered for scheduling.
		log.Infof(
			"no resources with non-zero weight available for scheduling in NodeDb: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.gangScheduler.nodeDb.TotalResources(),
		)
		return &SchedulerResult{}, nil
	}
	nodeIdByJobId := make(map[string]string)
	scheduledJobs := make([]interfaces.LegacySchedulerJob, 0)
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
		if len(gctx.JobSchedulingContexts) == 0 {
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
			for _, jctx := range gctx.JobSchedulingContexts {
				scheduledJobs = append(scheduledJobs, jctx.Job)
				pctx := jctx.PodSchedulingContext
				if pctx != nil && pctx.NodeId != "" {
					nodeIdByJobId[jctx.JobId] = pctx.NodeId
				}
			}
		} else if schedulerconstraints.IsTerminalUnschedulableReason(unschedulableReason) {
			// If unschedulableReason indicates no more new jobs can be scheduled,
			// instruct the underlying iterator to only yield evicted jobs from now on.
			sch.candidateGangIterator.OnlyYieldEvicted()
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
	return &SchedulerResult{
		PreemptedJobs:      nil,
		ScheduledJobs:      scheduledJobs,
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
		if !isEvictedJob(job) {
			// Rescheduled jobs don't count towards the limit.
			it.jobsSeen++
		}
		if it.hitLookbackLimit() {
			return nil, nil
		}

		// Skip this job if it's known to be unschedulable.
		if len(it.schedulingContext.UnfeasibleSchedulingKeys) > 0 {
			schedulingKey := it.schedulingContext.SchedulingKeyFromLegacySchedulerJob(job)
			if unsuccessfulJctx, ok := it.schedulingContext.UnfeasibleSchedulingKeys[schedulingKey]; ok {
				jctx := &schedulercontext.JobSchedulingContext{
					Created:              time.Now(),
					JobId:                job.GetId(),
					Job:                  job,
					UnschedulableReason:  unsuccessfulJctx.UnschedulableReason,
					PodSchedulingContext: unsuccessfulJctx.PodSchedulingContext,
				}
				if _, err := it.schedulingContext.AddJobSchedulingContext(jctx); err != nil {
					return nil, err
				}
				continue
			}
		}

		gangId, gangCardinality, isGangJob, err := GangIdAndCardinalityFromAnnotations(job.GetAnnotations())
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
					schedulercontext.JobSchedulingContextsFromJobs(
						it.schedulingContext.PriorityClasses,
						gang,
					),
				)
				return it.next, nil
			}
		} else {
			it.next = schedulercontext.NewGangSchedulingContext(
				schedulercontext.JobSchedulingContextsFromJobs(
					it.schedulingContext.PriorityClasses,
					[]interfaces.LegacySchedulerJob{job},
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
	// If true, this iterator only yields gangs where all jobs are evicted.
	onlyYieldEvicted bool
	// Reusable buffer to avoid allocations.
	buffer schedulerobjects.ResourceList
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq QueueCandidateGangIteratorPQ
}

func NewCandidateGangIterator(
	sctx *schedulercontext.SchedulingContext,
	iteratorsByQueue map[string]*QueuedGangIterator,
) (*CandidateGangIterator, error) {
	it := &CandidateGangIterator{
		SchedulingContext: sctx,
		buffer:            schedulerobjects.NewResourceListWithDefaultSize(),
		pq:                make(QueueCandidateGangIteratorPQ, 0, len(iteratorsByQueue)),
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
		// We assume here that all evicted jobs appear before non-evicted jobs in the queue.
		// Hence, it's safe to drop a queue once a non-evicted job has been seen.
		return false, nil
	}
	heap.Push(&it.pq, item)
	return true, nil
}

func (it *CandidateGangIterator) updatePQItem(item *QueueCandidateGangIteratorItem) error {
	item.gctx = nil
	item.fractionOfFairShare = 0
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
	item.fractionOfFairShare = it.fractionOfFairShareWithGctx(gctx)
	return nil
}

// fractionOfFairShareWithGctx returns the fraction of its fair share this queue would have if the jobs in gctx were scheduled.
func (it *CandidateGangIterator) fractionOfFairShareWithGctx(gctx *schedulercontext.GangSchedulingContext) float64 {
	qctx := it.SchedulingContext.QueueSchedulingContexts[gctx.Queue]
	it.buffer.Zero()
	it.buffer.Add(qctx.Allocated)
	it.buffer.Add(gctx.TotalResourceRequests)
	return qctx.TotalCostForQueueWithAllocation(it.buffer)
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
	for len(it.pq) > 0 && it.onlyYieldEvicted && !it.pq[0].gctx.AllJobsEvicted {
		heap.Pop(&it.pq)
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
