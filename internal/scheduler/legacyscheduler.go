package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type LegacySchedulerJob interface {
	GetId() string
	GetQueue() string
	GetAnnotations() map[string]string
	GetRequirements(map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo
}

// SchedulingConstraints collects scheduling constraints,
// e.g., per-queue resource limits.
type SchedulingConstraints struct {
	Priorities      []int32
	PriorityClasses map[string]configuration.PriorityClass
	// Executor for which we're currently scheduling jobs.
	ExecutorId string
	// Resource pool of this executor.
	Pool string
	// Weights used when computing total resource usage.
	ResourceScarcity map[string]float64
	// Max number of jobs to scheduler per lease jobs call.
	MaximumJobsToSchedule uint
	// Max number of consecutive unschedulable jobs to consider for a queue before giving up.
	MaxConsecutiveUnschedulableJobs uint
	// Jobs leased to this executor must be at least this large.
	// Used, e.g., to avoid scheduling CPU-only jobs onto clusters with GPUs.
	MinimumJobSize schedulerobjects.ResourceList
	// Per-queue resource limits.
	// Map from resource type to the limit for that resource.
	MaximalResourceFractionPerQueue map[string]float64
	// Limit- as a fraction of total resources across worker clusters- of resource types at each priority.
	// The limits are cumulative, i.e., the limit at priority p includes all higher levels.
	MaximalCumulativeResourceFractionPerQueueAndPriority map[int32]map[string]float64
	// Max resources to schedule per queue at a time.
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	// Max resources to schedule at a time.
	MaximalResourceFractionToSchedule map[string]float64
	// Total resources across all worker clusters.
	// Used when computing resource limits.
	TotalResources schedulerobjects.ResourceList
}

func SchedulingConstraintsFromSchedulingConfig(
	executorId, pool string,
	minimumJobSize schedulerobjects.ResourceList,
	config configuration.SchedulingConfig,
	totalResources schedulerobjects.ResourceList,
) *SchedulingConstraints {
	priorities := make([]int32, 0)
	maximalCumulativeResourceFractionPerQueueAndPriority := make(map[int32]map[string]float64, 0)
	for _, priority := range config.Preemption.PriorityClasses {
		// priorities = append(priorities, priority.Priority)
		maximalCumulativeResourceFractionPerQueueAndPriority[priority.Priority] = priority.MaximalResourceFractionPerQueue
	}
	if len(priorities) == 0 {
		priorities = []int32{0}
	}
	return &SchedulingConstraints{
		Priorities:       priorities,
		PriorityClasses:  config.Preemption.PriorityClasses,
		ExecutorId:       executorId,
		Pool:             pool,
		ResourceScarcity: config.GetResourceScarcity(pool),

		MaximumJobsToSchedule:                                config.MaximumJobsToSchedule,
		MaxConsecutiveUnschedulableJobs:                      config.QueueLeaseBatchSize,
		MinimumJobSize:                                       minimumJobSize,
		MaximalResourceFractionPerQueue:                      config.MaximalResourceFractionPerQueue,
		MaximalCumulativeResourceFractionPerQueueAndPriority: maximalCumulativeResourceFractionPerQueueAndPriority,
		MaximalResourceFractionToSchedulePerQueue:            config.MaximalResourceFractionToSchedulePerQueue,
		MaximalResourceFractionToSchedule:                    config.MaximalClusterFractionToSchedule,
		TotalResources:                                       totalResources,
	}
}

// SchedulerJobRepository represents the underlying jobs database.
type SchedulerJobRepository[T LegacySchedulerJob] interface {
	// GetJobIterator returns a iterator over queued jobs for a given queue.
	GetJobIterator(ctx context.Context, queue string) (JobIterator[T], error)
	// TryLeaseJobs tries to create jobs leases and returns the jobs that were successfully leased.
	// Leasing may fail, e.g., if the job was concurrently leased to another executor.
	TryLeaseJobs(clusterId string, queue string, jobs []T) ([]T, error)
}

type JobIterator[T LegacySchedulerJob] interface {
	Next() (T, error)
}

// QueuedGangIterator is an iterator over all gangs in a queue,
// where a gang is a set of jobs for which the gangIdAnnotation has equal value.
// A gang is yielded once the final member of the gang has been received.
// Jobs without gangIdAnnotation are considered to be gangs of cardinality 1.
type QueuedGangIterator[T LegacySchedulerJob] struct {
	ctx                context.Context
	queuedJobsIterator JobIterator[T]
	// Jobs are grouped into gangs by this annotation.
	gangIdAnnotation string
	// Jobs in a gang must specify the total number of jobs in the gang via this annotation.
	gangCardinalityAnnotation string
	// Groups jobs by the gang they belong to.
	jobsByGangId map[string][]T
	next         []T
}

func NewQueuedGangIterator[T LegacySchedulerJob](ctx context.Context, it JobIterator[T], gangIdAnnotation, gangCardinalityAnnotation string) *QueuedGangIterator[T] {
	return &QueuedGangIterator[T]{
		ctx:                       ctx,
		queuedJobsIterator:        it,
		gangIdAnnotation:          gangIdAnnotation,
		gangCardinalityAnnotation: gangCardinalityAnnotation,
		jobsByGangId:              make(map[string][]T),
	}
}

func (it *QueuedGangIterator[T]) Next() ([]T, error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func (it *QueuedGangIterator[T]) Clear() error {
	it.next = nil
	return nil
}

func (it *QueuedGangIterator[T]) Peek() ([]T, error) {
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

		// TODO: Add when we've removed T.
		// if job == nil {
		// 	return nil, nil
		// }
		gangId, gangCardinality, isGangJob, err := GangIdAndCardinalityFromAnnotations(
			job.GetAnnotations(),
			it.gangIdAnnotation,
			it.gangCardinalityAnnotation,
		)
		if err != nil {
			log := ctxlogrus.Extract(it.ctx)
			logging.WithStacktrace(log, err).Errorf("failed to get gang cardinality for job %s", job.GetId())
			gangCardinality = 1 // Schedule jobs with invalid gang cardinality one by one.
		}
		if isGangJob {
			it.jobsByGangId[gangId] = append(it.jobsByGangId[gangId], job)
			gang := it.jobsByGangId[gangId]
			if len(gang) == gangCardinality {
				delete(it.jobsByGangId, gangId)
				it.next = gang
				return it.next, nil
			}
		} else {
			it.next = []T{job}
			return it.next, nil
		}
	}
}

// QueueCandidateGangIterator is an iterator over gangs in a queue that could be scheduled
// without exceeding per-queue limits.
type QueueCandidateGangIterator[T LegacySchedulerJob] struct {
	ctx context.Context
	SchedulingConstraints
	QueueSchedulingRoundReport *QueueSchedulingRoundReport[T]
	queuedGangIterator         *QueuedGangIterator[T]
}

func (it *QueueCandidateGangIterator[T]) Next() ([]*JobSchedulingReport[T], error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func (it *QueueCandidateGangIterator[T]) Clear() error {
	if err := it.queuedGangIterator.Clear(); err != nil {
		return err
	}
	return nil
}

func (it *QueueCandidateGangIterator[T]) Peek() ([]*JobSchedulingReport[T], error) {
	var consecutiveUnschedulableJobs uint
	for gang, err := it.queuedGangIterator.Peek(); gang != nil; gang, err = it.queuedGangIterator.Peek() {
		if err != nil {
			return nil, err
		}
		if it.MaxConsecutiveUnschedulableJobs != 0 && consecutiveUnschedulableJobs == it.MaxConsecutiveUnschedulableJobs {
			break
		}
		if v, ok, err := it.f(gang); err != nil {
			return nil, err
		} else if ok {
			return v, nil
		}
		if err := it.queuedGangIterator.Clear(); err != nil {
			return nil, err
		}
		consecutiveUnschedulableJobs++
	}
	return nil, nil
}

func (it *QueueCandidateGangIterator[T]) f(gang []T) ([]*JobSchedulingReport[T], bool, error) {
	if gang == nil {
		return nil, false, nil
	}
	reports, err := it.schedulingReportsFromJobs(it.ctx, gang)
	if err != nil {
		return nil, false, err
	}
	unschedulableReason := ""
	for _, report := range reports {
		if report.UnschedulableReason != "" {
			unschedulableReason = report.UnschedulableReason
			break
		}
	}
	if unschedulableReason != "" {
		for _, report := range reports {
			it.QueueSchedulingRoundReport.AddJobSchedulingReport(report)
		}
	}
	return reports, unschedulableReason == "", nil
}

func (it *QueueCandidateGangIterator[T]) schedulingReportsFromJobs(ctx context.Context, jobs []T) ([]*JobSchedulingReport[T], error) {
	if jobs == nil {
		return nil, nil
	}
	if len(jobs) == 0 {
		return make([]*JobSchedulingReport[T], 0), nil
	}

	// Create the scheduling reports and calculate the total requests of the gang
	// We consider the total resource requests of a gang
	// to be the sum of the requests over all jobs in the gang.
	reports := make([]*JobSchedulingReport[T], len(jobs))

	gangTotalResourceRequests := schedulerobjects.ResourceList{}
	for _, job := range jobs {
		if req := job.GetRequirements(it.PriorityClasses); req != nil {
			gangTotalResourceRequests.Add(req.GetTotalResourceRequest())
		}
	}

	totalResourceRequestsFromJobs[T](jobs, it.PriorityClasses)
	timestamp := time.Now()
	for i, job := range jobs {
		jobId, err := uuidFromUlidString(job.GetId())
		if err != nil {
			return nil, err
		}
		req := PodRequirementFromJobSchedulingInfo(job.GetRequirements(it.PriorityClasses))
		if err != nil {
			return nil, err
		}
		reports[i] = &JobSchedulingReport[T]{
			Timestamp:  timestamp,
			JobId:      jobId,
			Job:        job,
			Req:        req,
			ExecutorId: it.ExecutorId,
		}

		gangTotalResourceRequests.Add(schedulerobjects.ResourceList{
			Resources: armadaresource.FromResourceList(req.ResourceRequirements.Requests),
		})
	}

	// Set the unschedulableReason of all reports before returning.
	// If any job in a gang fails to schedule,
	// we assign the unschedulable reason of that job to all jobs in the gang.
	unschedulableReason := ""
	defer func() {
		for _, report := range reports {
			report.UnschedulableReason = unschedulableReason
		}
	}()

	// We assume that all jobs in a gang have the same priority class
	// (which we enforce at job submission).
	priority := reports[0].Req.Priority

	// Check that the job is large enough for this executor.
	if ok, reason := jobIsLargeEnough(gangTotalResourceRequests, it.MinimumJobSize); !ok {
		unschedulableReason = reason
		return reports, nil
	}

	// MaximalResourceFractionToSchedulePerQueue check.
	roundQueueResourcesByPriority := it.QueueSchedulingRoundReport.ScheduledResourcesByPriority.DeepCopy()
	roundQueueResourcesByPriority.AddResouceList(priority, gangTotalResourceRequests)
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		roundQueueResourcesByPriority.AggregateByResource(),
		it.SchedulingConstraints.TotalResources,
		it.MaximalResourceFractionToSchedulePerQueue,
	); exceeded {
		unschedulableReason = reason + " (per scheduling round limit for this queue)"
		return reports, nil
	}

	// MaximalResourceFractionPerQueue check.
	totalQueueResourcesByPriority := it.QueueSchedulingRoundReport.InitialResourcesByPriority.DeepCopy()
	totalQueueResourcesByPriority.Add(roundQueueResourcesByPriority)
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		totalQueueResourcesByPriority.AggregateByResource(),
		it.SchedulingConstraints.TotalResources,
		it.MaximalResourceFractionPerQueue,
	); exceeded {
		unschedulableReason = reason + " (total limit for this queue)"
		return reports, nil
	}

	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
	if exceeded, reason := exceedsPerPriorityResourceLimits(
		ctx,
		priority,
		totalQueueResourcesByPriority,
		it.SchedulingConstraints.TotalResources,
		it.MaximalCumulativeResourceFractionPerQueueAndPriority,
	); exceeded {
		unschedulableReason = reason + " (total limit for this queue)"
		return reports, nil
	}

	return reports, nil
}

func totalResourceRequestsFromJobs[T LegacySchedulerJob](jobs []T, priorityClasses map[string]configuration.PriorityClass) schedulerobjects.ResourceList {
	rv := schedulerobjects.ResourceList{}
	for _, job := range jobs {
		for _, reqs := range job.GetRequirements(priorityClasses).GetObjectRequirements() {
			rv.Add(
				schedulerobjects.ResourceListFromV1ResourceList(
					reqs.GetPodRequirements().ResourceRequirements.Requests,
				),
			)
		}
	}
	return rv
}

// Priority queue used by CandidateGangIterator to determine from which queue to schedule the next job.
type QueueCandidateGangIteratorPQ[T LegacySchedulerJob] []*QueueCandidateGangIteratorItem[T]

type QueueCandidateGangIteratorItem[T LegacySchedulerJob] struct {
	// Each item corresponds to a queue.
	queue string
	// Iterator for this queue.
	it *QueueCandidateGangIterator[T]
	// The most recent value produced by the iterator.
	// nextGang []*JobSchedulingReport[T]
	// The priority of the item in the queue.
	// A non-negative value expressing what fraction of their fair share
	// this user would have if the next schedulable job in this queue were to be scheduled.
	//
	// TODO: Consider renaming.
	priority float64
	// The index of the item in the heap.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int
}

func (pq QueueCandidateGangIteratorPQ[T]) Len() int { return len(pq) }

func (pq QueueCandidateGangIteratorPQ[T]) Less(i, j int) bool {
	// return pq[i].priority.Cmp(pq[j].priority) == -1
	return pq[i].priority < pq[j].priority
}

func (pq QueueCandidateGangIteratorPQ[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *QueueCandidateGangIteratorPQ[T]) Push(x any) {
	n := len(*pq)
	item := x.(*QueueCandidateGangIteratorItem[T])
	item.index = n
	*pq = append(*pq, item)
}

func (pq *QueueCandidateGangIteratorPQ[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// CandidateGangIterator multiplexes between queues.
// Responsible for maintaining fair share and enforcing cross-queue scheduling constraints.
type CandidateGangIterator[T LegacySchedulerJob] struct {
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport[T]
	ctx                   context.Context
	// These factors influence the fraction of resources assigned to each queue.
	priorityFactorByQueue map[string]float64
	// Inverse of the priority factor for
	weightByQueue map[string]float64
	// Sum of all weights.
	weightSum float64
	// Priority queue of per-queue iterators.
	pq QueueCandidateGangIteratorPQ[T]
}

func NewCandidateGangIterator[T LegacySchedulerJob](
	schedulingConstraints SchedulingConstraints,
	schedulingRoundReport *SchedulingRoundReport[T],
	ctx context.Context,
	iteratorsByQueue map[string]*QueueCandidateGangIterator[T],
	priorityFactorByQueue map[string]float64,
) (*CandidateGangIterator[T], error) {
	if len(iteratorsByQueue) != len(priorityFactorByQueue) {
		return nil, errors.Errorf("iteratorsByQueue and priorityFactorByQueue are not of equal length")
	}
	weightSum := 0.0
	weightByQueue := make(map[string]float64)
	for queue, priorityFactor := range priorityFactorByQueue {
		if _, ok := iteratorsByQueue[queue]; !ok {
			return nil, errors.Errorf("no iterator found for queue %s", queue)
		}
		weight := 1 / max(priorityFactor, 1)
		weightByQueue[queue] = weight
		weightSum += weight
	}
	rv := &CandidateGangIterator[T]{
		SchedulingConstraints: schedulingConstraints,
		SchedulingRoundReport: schedulingRoundReport,
		ctx:                   ctx,
		priorityFactorByQueue: priorityFactorByQueue,
		weightByQueue:         weightByQueue,
		weightSum:             weightSum,
		pq:                    make(QueueCandidateGangIteratorPQ[T], 0, len(iteratorsByQueue)),
	}
	for queue, queueIt := range iteratorsByQueue {
		if err := rv.pushToPQ(queue, queueIt); err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func (it *CandidateGangIterator[T]) pushToPQ(queue string, queueIt *QueueCandidateGangIterator[T]) error {
	reports, err := queueIt.Peek()
	if err != nil {
		return err
	}
	if reports == nil {
		return nil
	}
	gang := make([]T, len(reports))
	for i, report := range reports {
		gang[i] = report.Job
	}
	initialResourcesForQueue := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue].InitialResourcesByPriority
	scheduledResourcesForQueue := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue].ScheduledResourcesByPriority
	totalResourcesForQueue := initialResourcesForQueue.DeepCopy()
	totalResourcesForQueue.Add(scheduledResourcesForQueue)
	totalResourcesForQueueWithGang := totalResourcesForQueue.AggregateByResource()
	totalResourcesForQueueWithGang.Add(totalResourceRequestsFromJobs[T](gang, it.PriorityClasses))
	fairShare := it.weightByQueue[queue] / it.weightSum
	v := fractionOfFairShare(
		fairShare,
		totalResourcesForQueueWithGang,
		it.ResourceScarcity,
		it.TotalResources,
	)
	item := &QueueCandidateGangIteratorItem[T]{
		queue:    queue,
		it:       queueIt,
		priority: v,
	}
	it.pq.Push(item)
	return nil
}

func (it *CandidateGangIterator[T]) Next() ([]*JobSchedulingReport[T], error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func (it *CandidateGangIterator[T]) Clear() error {
	if len(it.pq) == 0 {
		return nil
	}
	item := (it.pq.Pop()).(QueueCandidateGangIteratorItem[T])
	if err := item.it.Clear(); err != nil {
		return err
	}
	if err := it.pushToPQ(item.queue, item.it); err != nil {
		return err
	}
	return nil
}

// TODO: We call peek() on the underlying iterators more often than needed.
func (it *CandidateGangIterator[T]) Peek() ([]*JobSchedulingReport[T], error) {
	if it.MaximumJobsToSchedule != 0 && it.SchedulingRoundReport.NumScheduledJobs == int(it.MaximumJobsToSchedule) {
		it.SchedulingRoundReport.TerminationReason = "maximum number of jobs scheduled"
		return nil, nil
	}

	// Yield a gang.
	currentQueue := ""
	for {
		if len(it.pq) == 0 {
			// No queued jobs left.
			return nil, nil
		}
		item := (it.pq.Pop()).(QueueCandidateGangIteratorItem[T])
		if item.queue != currentQueue {
			currentQueue = item.queue
			if err := it.pushToPQ(item.queue, item.it); err != nil {
				return nil, err
			}
			continue
		}
		reports, err := item.it.Peek()
		if err != nil {
			return nil, err
		}

		if v, ok, err := it.f(reports); err != nil {
			return nil, err
		} else if ok {
			if err := it.pushToPQ(item.queue, item.it); err != nil {
				return nil, err
			}
			return v, nil
		}

		if err := item.it.Clear(); err != nil {
			return nil, err
		}
		if err := it.pushToPQ(item.queue, item.it); err != nil {
			return nil, err
		}
	}
}

// func (it *CandidateGangIterator[T]) NextOld() ([]*JobSchedulingReport[T], error) {
// 	if it.MaximumJobsToSchedule != 0 && it.SchedulingRoundReport.NumScheduledJobs == int(it.MaximumJobsToSchedule) {
// 		it.SchedulingRoundReport.TerminationReason = "maximum number of jobs scheduled"
// 		return nil, nil
// 	}

// 	// Aggregate resource usage by queue.
// 	// Used to fairly share resources between queues.
// 	aggregatedResourceUsageByQueue := make(map[string]schedulerobjects.ResourceList)
// 	for queue := range it.priorityFactorByQueue {
// 		if report := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue]; report != nil {
// 			rl := report.InitialResourcesByPriority.AggregateByResource()
// 			rl.Add(report.ScheduledResourcesByPriority.AggregateByResource())
// 			aggregatedResourceUsageByQueue[queue] = rl
// 		}
// 	}

// 	// Yield a gang.
// 	for {
// 		// First, select which queue to schedule from.
// 		// Queues below their fair share are selected with higher probability.
// 		var queue string
// 		var queueIt *QueueCandidateGangIterator[T]
// 		for {
// 			if len(it.priorityFactorByQueue) == 0 {
// 				// No queued jobs left.
// 				return nil, nil
// 			}
// 			weights := queueSelectionWeights(
// 				it.priorityFactorByQueue,
// 				aggregatedResourceUsageByQueue,
// 				it.ResourceScarcity,
// 			)
// 			queue, _ = pickQueueRandomly(weights, it.rand)

// 			if iter := it.iteratorsByQueue[queue]; iter != nil {
// 				queueIt = iter
// 				break
// 			} else {
// 				log.Errorf("iterator missing for queue %s", queue)
// 				delete(it.priorityFactorByQueue, queue)
// 			}
// 		}

// 		// Then, find a gang from that queue that could potentially be scheduled.
// 		for reports, err := queueIt.Next(); reports != nil; reports, err = queueIt.Next() {
// 			if err != nil {
// 				return nil, err
// 			}

// 			// Check overall per-round resource limits.
// 			totalScheduledResources := it.SchedulingRoundReport.ScheduledResourcesByPriority.AggregateByResource()
// 			gangResourceRequests := schedulerobjects.ResourceList{}
// 			for _, report := range reports {
// 				gangResourceRequests.Add(schedulerobjects.ResourceListFromV1ResourceList(report.Req.ResourceRequirements.Requests))
// 			}
// 			totalScheduledResources.Add(gangResourceRequests)
// 			if exceeded, reason := exceedsResourceLimits(
// 				it.ctx,
// 				totalScheduledResources,
// 				it.SchedulingConstraints.TotalResources,
// 				it.MaximalResourceFractionToSchedule,
// 			); exceeded {
// 				unschedulableReason := reason + " (overall per scheduling round limit)"
// 				for _, report := range reports {
// 					report.UnschedulableReason = unschedulableReason
// 					it.SchedulingRoundReport.AddJobSchedulingReport(report)
// 				}
// 			} else {
// 				return reports, nil
// 			}
// 		}

// 		// No more jobs to process for this queue.
// 		delete(it.priorityFactorByQueue, queue)
// 	}
// }

func (it *CandidateGangIterator[T]) f(reports []*JobSchedulingReport[T]) ([]*JobSchedulingReport[T], bool, error) {
	totalScheduledResources := it.SchedulingRoundReport.ScheduledResourcesByPriority.AggregateByResource()
	gangResourceRequests := schedulerobjects.ResourceList{}
	for _, report := range reports {
		gangResourceRequests.Add(schedulerobjects.ResourceListFromV1ResourceList(report.Req.ResourceRequirements.Requests))
	}
	totalScheduledResources.Add(gangResourceRequests)
	if exceeded, reason := exceedsResourceLimits(
		it.ctx,
		totalScheduledResources,
		it.SchedulingConstraints.TotalResources,
		it.MaximalResourceFractionToSchedule,
	); exceeded {
		unschedulableReason := reason + " (overall per scheduling round limit)"
		for _, report := range reports {
			report.UnschedulableReason = unschedulableReason
			it.SchedulingRoundReport.AddJobSchedulingReport(report)
		}
		return reports, false, nil
	} else {
		return reports, true, nil
	}
}

// func PriorityFromJob(job *api.Job, priorityByPriorityClassName map[string]configuration.PriorityClass) (priority int32, ok bool) {
// 	return adapters.PriorityFromPodSpec(util.PodSpecFromJob(job), priorityByPriorityClassName)
// }

func uuidFromUlidString(ulid string) (uuid.UUID, error) {
	protoUuid, err := armadaevents.ProtoUuidFromUlidString(ulid)
	if err != nil {
		return uuid.UUID{}, err
	}
	return armadaevents.UuidFromProtoUuid(protoUuid), nil
}

// exceedsResourceLimits returns true if used[t]/total[t] > limits[t] for some resource t,
// and, if that is the case, a string indicating which resource limit was exceeded.
func exceedsResourceLimits(_ context.Context, used, total schedulerobjects.ResourceList, limits map[string]float64) (bool, string) {
	for resourceType, limit := range limits {
		totalAmount := total.Get(resourceType)
		usedAmount := used.Get(resourceType)
		if armadaresource.QuantityAsFloat64(usedAmount)/armadaresource.QuantityAsFloat64(totalAmount) > limit {
			return true, fmt.Sprintf("scheduling would exceed %s quota", resourceType)
		}
	}
	return false, ""
}

// Check if scheduling this job would exceed per-priority-per-queue resource limits.
func exceedsPerPriorityResourceLimits(ctx context.Context, jobPriority int32, usedByPriority schedulerobjects.QuantityByPriorityAndResourceType, total schedulerobjects.ResourceList, limits map[int32]map[string]float64) (bool, string) {
	// Calculate cumulative usage at each priority.
	// This involves summing the usage at all higher priorities.
	priorities := maps.Keys(limits)
	slices.Sort(priorities)
	cumulativeUsageByPriority := make(schedulerobjects.QuantityByPriorityAndResourceType)
	cumulativeSum := schedulerobjects.ResourceList{}
	for i := len(priorities) - 1; i >= 0; i-- {
		priority := priorities[i]
		cumulativeSum.Add(usedByPriority[priority])
		cumulativeUsageByPriority[priority] = cumulativeSum.DeepCopy()
	}
	for priority, priorityLimits := range limits {
		if priority <= jobPriority {
			if rl, ok := cumulativeUsageByPriority[priority]; ok {
				limitExceeded, msg := exceedsResourceLimits(ctx, rl, total, priorityLimits)
				if limitExceeded {
					return true, fmt.Sprintf("%s at priority %d", msg, priority)
				}
			} else {
				log := ctxlogrus.Extract(ctx)
				log.Warnf("Job scheduled at priority %d but there are no per-priority limits set up for this class; skipping per periority limit check", priority)
			}
		}
	}
	return false, ""
}

// Check that this job is at least equal to the minimum job size.
func jobIsLargeEnough(jobTotalResourceRequests, minimumJobSize schedulerobjects.ResourceList) (bool, string) {
	if len(minimumJobSize.Resources) == 0 {
		return true, ""
	}
	if len(jobTotalResourceRequests.Resources) == 0 {
		return true, ""
	}
	for resourceType, limit := range minimumJobSize.Resources {
		q := jobTotalResourceRequests.Get(resourceType)
		if limit.Cmp(q) == 1 {
			return false, fmt.Sprintf(
				"job requests %s %s, but the minimum is %s",
				q.String(), resourceType, limit.String(),
			)
		}
	}
	return true, ""
}

// func PodRequirementsFromJobs[T LegacySchedulerJob](priorityClasses map[string]configuration.PriorityClass, jobs []T) ([]*schedulerobjects.PodRequirements, error) {
// 	rv := make([]*schedulerobjects.PodRequirements, 0, len(jobs))
// 	for _, job := range jobs {
// 		req, err := PodRequirementsFromJob(job, priorityClasses)
// 		if err != nil {
// 			return nil, err
// 		}
// 		rv = append(rv, req)
// 	}
// 	return rv, nil
// }

type LegacyScheduler[T LegacySchedulerJob] struct {
	ctx context.Context
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport[T]
	CandidateGangIterator *CandidateGangIterator[T]
	// Contains all nodes to be considered for scheduling.
	// Used for matching pods with nodes.
	NodeDb *NodeDb
	// Used to request jobs from Redis and to mark jobs as leased.
	JobRepository SchedulerJobRepository[T]
	// Jobs are grouped into gangs by this annotation.
	GangIdAnnotation string
	// Jobs in a gang specify the number of jobs in the gang via this annotation.
	GangCardinalityAnnotation string
}

func (sched *LegacyScheduler[T]) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Executor:\t%s\n", sched.ExecutorId)
	if len(sched.SchedulingConstraints.TotalResources.Resources) == 0 {
		fmt.Fprint(w, "Total resources:\tnone\n")
	} else {
		fmt.Fprint(w, "Total resources:\n")
		for t, q := range sched.SchedulingConstraints.TotalResources.Resources {
			fmt.Fprintf(w, "  %s: %s\n", t, q.String())
		}
	}
	fmt.Fprintf(w, "Minimum job size:\t%v\n", sched.MinimumJobSize)
	if sched.NodeDb == nil {
		fmt.Fprintf(w, "NodeDb:\t%v\n", sched.NodeDb)
	} else {
		fmt.Fprint(w, "NodeDb:\n")
		fmt.Fprint(w, indent.String("\t", sched.NodeDb.String()))
	}
	w.Flush()
	return sb.String()
}

func NewLegacyScheduler[T LegacySchedulerJob](
	ctx context.Context,
	constraints SchedulingConstraints,
	config configuration.SchedulingConfig,
	nodeDb *NodeDb,
	jobRepository SchedulerJobRepository[T],
	priorityFactorByQueue map[string]float64,
	initialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) (*LegacyScheduler[T], error) {
	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, constraints.TotalResources) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		return nil, errors.New("no resources with non-zero weight available for scheduling")
	}
	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, nodeDb.totalResources) == 0 {
		// This refers to the resources currently considered for schedling.
		return nil, errors.New("no resources with non-zero weight available for scheduling in NodeDb")
	}

	schedulingRoundReport := NewSchedulingRoundReport[T](
		constraints.TotalResources,
		priorityFactorByQueue,
		initialResourcesByQueueAndPriority,
	)

	// Per-queue iterator pipelines.
	iteratorsByQueue := make(map[string]*QueueCandidateGangIterator[T])
	for queue := range priorityFactorByQueue {
		// Load jobs from Redis.
		queuedJobsIterator, err := jobRepository.GetJobIterator(ctx, queue)
		if err != nil {
			return nil, err
		}

		// Group jobs into gangs, to be scheduled together.
		queuedGangIterator := NewQueuedGangIterator[T](
			ctx,
			queuedJobsIterator,
			config.GangIdAnnotation,
			config.GangCardinalityAnnotation,
		)

		// Enforce per-queue constraints.
		iteratorsByQueue[queue] = &QueueCandidateGangIterator[T]{
			SchedulingConstraints:      constraints,
			QueueSchedulingRoundReport: schedulingRoundReport.QueueSchedulingRoundReports[queue],
			ctx:                        ctx,
			queuedGangIterator:         queuedGangIterator,
		}
	}

	// Multiplex between queues and enforce cross-queue constraints.
	candidateGangIterator, err := NewCandidateGangIterator[T](
		constraints,
		schedulingRoundReport,
		ctx,
		iteratorsByQueue,
		maps.Clone(priorityFactorByQueue),
	)
	if err != nil {
		return nil, err
	}

	return &LegacyScheduler[T]{
		ctx:                   ctx,
		SchedulingConstraints: constraints,
		SchedulingRoundReport: schedulingRoundReport,
		CandidateGangIterator: candidateGangIterator,
		NodeDb:                nodeDb,
		JobRepository:         jobRepository,
	}, nil
}

func (sched *LegacyScheduler[T]) Schedule() ([]T, error) {
	defer func() {
		sched.SchedulingRoundReport.Finished = time.Now()
	}()

	jobsToLeaseByQueue := make(map[string][]T, 0)
	numJobsToLease := 0
	for reports, err := sched.CandidateGangIterator.Next(); reports != nil; reports, err = sched.CandidateGangIterator.Next() {
		if err != nil {
			sched.SchedulingRoundReport.TerminationReason = err.Error()
			return nil, err
		}
		if len(reports) == 0 {
			continue
		}
		select {
		case <-sched.ctx.Done():
			sched.SchedulingRoundReport.TerminationReason = sched.ctx.Err().Error()
			return nil, err
		default:
		}

		jobs := make([]T, len(reports))
		for i, r := range reports {
			jobs[i] = r.Job
		}

		reqs := PodRequirementsFromLegacySchedulerJobs(jobs, sched.PriorityClasses)
		podSchedulingReports, ok, err := sched.NodeDb.ScheduleMany(reqs)
		if err != nil {
			return nil, err
		}
		for _, r := range reports {
			// Store all pod scheduling reports for all jobs in the gang.
			r.PodSchedulingReports = podSchedulingReports
		}
		if !ok {
			if len(reports) > 0 {
				for _, r := range reports {
					r.UnschedulableReason = "at least one pod in the gang did not fit on any Node"
				}
			} else {
				for _, r := range reports {
					r.UnschedulableReason = "pod does not fit on any Node"
				}
			}
			for _, r := range reports {
				sched.SchedulingRoundReport.AddJobSchedulingReport(r)
			}
		} else {
			for _, r := range reports {
				jobsToLeaseByQueue[r.Job.GetQueue()] = append(jobsToLeaseByQueue[r.Job.GetQueue()], r.Job)
				sched.SchedulingRoundReport.AddJobSchedulingReport(r)
			}
			numJobsToLease += len(reports)
		}
	}
	sched.SchedulingRoundReport.TerminationReason = "no remaining schedulable jobs"
	rv := make([]T, 0)
	for _, jobs := range jobsToLeaseByQueue {
		rv = append(rv, jobs...)
	}
	return rv, nil
}

func GangIdAndCardinalityFromLegacySchedulerJob(job LegacySchedulerJob, gangIdAnnotation, gangCardinalityAnnotation string, priorityClasses map[string]configuration.PriorityClass) (string, int, bool, error) {
	reqs := job.GetRequirements(priorityClasses)
	if reqs == nil {
		return "", 0, false, nil
	}
	if len(reqs.ObjectRequirements) != 1 {
		return "", 0, false, errors.Errorf("expected exactly one object requirement in %v", reqs)
	}
	podReqs := reqs.ObjectRequirements[0].GetPodRequirements()
	if reqs == nil {
		return "", 0, false, nil
	}
	return GangIdAndCardinalityFromAnnotations(
		podReqs.Annotations,
		gangIdAnnotation,
		gangCardinalityAnnotation,
	)
}

func GangIdAndCardinalityFromAnnotations(annotations map[string]string, gangIdAnnotation, gangCardinalityAnnotation string) (string, int, bool, error) {
	if annotations == nil {
		return "", 0, false, nil
	}
	gangId, ok := annotations[gangIdAnnotation]
	if !ok {
		return "", 0, false, nil
	}
	gangCardinalityString, ok := annotations[gangCardinalityAnnotation]
	if !ok {
		return "", 0, false, errors.Errorf("missing annotation %s", gangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return "", 0, false, errors.WithStack(err)
	}
	if gangCardinality <= 0 {
		return "", 0, false, errors.Errorf("gang cardinality is non-positive %d", gangCardinality)
	}
	return gangId, gangCardinality, true, nil
}

func queueSelectionWeights(priorityFactorByQueue map[string]float64, aggregateResourceUsageByQueue map[string]schedulerobjects.ResourceList, resourceScarcity map[string]float64) map[string]float64 {
	rv := make(map[string]float64)
	total := 0.0
	for _, rl := range aggregateResourceUsageByQueue {
		total += ResourceListAsWeightedApproximateFloat64(resourceScarcity, rl)
	}
	if total == 0 {
		// Avoid division by 0.
		total = 1
	}
	inversePriorityFactorsSum := 0.0
	for _, priorityFactor := range priorityFactorByQueue {
		if priorityFactor < 1 {
			// Avoid division by 0.
			priorityFactor = 1
		}
		inversePriorityFactorsSum += 1 / priorityFactor
	}
	weightsSum := 0.0
	for queue, priorityFactor := range priorityFactorByQueue {
		if priorityFactor < 1 {
			// Avoid division by 0.
			priorityFactor = 1
		}
		expected := 1 / priorityFactor / inversePriorityFactorsSum
		rl := aggregateResourceUsageByQueue[queue]
		usage := ResourceListAsWeightedApproximateFloat64(resourceScarcity, rl)
		if usage == 0 {
			// Avoid division by 0.
			usage = 1
		}
		actual := usage / total
		weight := expected / actual

		// Amplify weights to push queues towards their fair share.
		if weight < 1 {
			weight /= float64(len(priorityFactorByQueue))
		} else {
			weight *= float64(len(priorityFactorByQueue))
		}

		weightsSum += weight
		rv[queue] = weight
	}

	// Normalise
	for queue, weight := range rv {
		rv[queue] = weight / weightsSum
	}
	return rv
}

func getFairShareByQueue(weightByQueue map[string]float64) map[string]float64 {
	rv := make(map[string]float64)
	priorityFactorSum := 0.0
	for _, priorityFactor := range weightByQueue {
		priorityFactorSum += priorityFactor
	}
	priorityFactorSum = max(priorityFactorSum, 1)
	for queue, priorityFactor := range weightByQueue {
		rv[queue] = max(priorityFactor, 1e-6) / priorityFactorSum
	}
	return rv
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// func fractionOfFairShareWithGang[T LegacySchedulerJob](
// 	gang []T,
// 	fairShare float64,
// 	aggregateResourceUsage schedulerobjects.ResourceList,
// 	resourceScarcity map[string]float64,
// 	totalResources schedulerobjects.ResourceList,
// ) float64 {
// 	aggregateResourceUsage = aggregateResourceUsage.DeepCopy()
// 	for _, job := range gang {
// 		for _, reqs := range job.GetRequirements().GetObjectRequirements() {
// 			aggregateResourceUsage.Add(
// 				schedulerobjects.ResourceListFromV1ResourceList(
// 					reqs.GetPodRequirements().ResourceRequirements.Requests,
// 				),
// 			)
// 		}
// 	}
// 	return fractionOfFairShare(fairShare, aggregateResourceUsage, resourceScarcity, totalResources)
// }

func fractionOfFairShare(
	fairShare float64,
	aggregateResourceUsage schedulerobjects.ResourceList,
	resourceScarcity map[string]float64,
	totalResources schedulerobjects.ResourceList,
) float64 {
	used := ResourceListAsWeightedApproximateFloat64(resourceScarcity, aggregateResourceUsage)
	total := max(ResourceListAsWeightedApproximateFloat64(resourceScarcity, totalResources), 1)
	return (used / total) / fairShare
}

// func fractionOfFairShareWithGangOld(
// 	gang []LegacySchedulerJob,
// 	priorityFactorByQueue map[string]float64,
// 	aggregateResourceUsageByQueue map[string]schedulerobjects.ResourceList,
// 	resourceScarcity map[string]float64,
// 	totalResources schedulerobjects.ResourceList,
// ) map[string]float64 {
// 	rv := make(map[string]float64)
// 	total := ResourceListAsWeightedApproximateFloat64(resourceScarcity, totalResources)
// 	if total == 0 {
// 		// Avoid division by 0.
// 		total = 1
// 	}

// 	// For each queue, compute its total usage
// 	priorityFactorSum := 0.0
// 	for queue, priorityFactor := range priorityFactorByQueue {
// 		if priorityFactor < 1 {
// 			// Avoid division by 0.
// 			priorityFactor = 1
// 		}
// 		priorityFactorSum += priorityFactor

// 		// Resource usage if this gang were to be scheduled.
// 		rl := aggregateResourceUsageByQueue[queue]
// 		for _, job := range gang {
// 			for _, reqs := range job.GetRequirements().GetObjectRequirements() {
// 				rl.Add(
// 					schedulerobjects.ResourceListFromV1ResourceList(
// 						reqs.GetPodRequirements().ResourceRequirements.Requests,
// 					),
// 				)
// 			}
// 		}

// 		// Weighted sum of resource usage.
// 		usage := ResourceListAsWeightedApproximateFloat64(resourceScarcity, rl)
// 		if usage == 0 {
// 			// Avoid division by 0.
// 			usage = 1
// 		}
// 		rv[queue] = usage
// 	}

// 	// Normalise usage by fair share.
// 	for queue, usage := range rv {
// 		priorityFactor := priorityFactorByQueue[queue]
// 		if priorityFactor < 1 {
// 			// Avoid division by 0.
// 			priorityFactor = 1
// 		}
// 		fairShare := priorityFactor / priorityFactorSum
// 		actualShare := usage / total
// 		rv[queue] = actualShare / fairShare
// 	}
// 	return rv
// }

func ResourceListAsWeightedApproximateFloat64(resourceScarcity map[string]float64, rl schedulerobjects.ResourceList) float64 {
	usage := 0.0
	for resourceName, quantity := range rl.Resources {
		scarcity := resourceScarcity[resourceName]
		usage += armadaresource.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

// pickQueueRandomly returns a queue randomly selected from the provided map.
// The probability of returning a particular queue AQueue is shares[AQueue] / sharesSum,
// where sharesSum is the sum of all values in the provided map.
func pickQueueRandomly(weights map[string]float64, random *rand.Rand) (string, float64) {
	if len(weights) == 0 {
		return "", 0
	}

	// Generate a random number between 0 and sum.
	sum := 0.0
	for _, share := range weights {
		sum += share
	}
	var pick float64
	if random != nil {
		pick = sum * random.Float64()
	} else {
		pick = sum * rand.Float64()
	}

	// Iterate over queues in deterministic order.
	queues := maps.Keys(weights)
	slices.Sort(queues)

	// Select the queue as indicated by pick.
	current := 0.0
	for _, queue := range queues {
		share := weights[queue]
		current += share
		if current >= pick {
			return queue, share / sum
		}

	}
	log.Error("Could not randomly pick a queue, this should not happen!")
	queue := queues[len(queues)-1]
	return queue, weights[queue] / sum
}

// func extractSchedulerRequirements(j LegacySchedulerJob, pcs map[string]configuration.PriorityClass) (*schedulerobjects.PodRequirements, error) {
// 	switch job := j.(type) {
// 	case *api.Job:
// 		podSpec := util.PodSpecFromJob(job)
// 		if podSpec == nil {
// 			return nil, errors.New("failed to get pod spec")
// 		}
// 		return schedulerobjects.PodRequirementsFromPodSpec(
// 			podSpec,
// 			pcs,
// 		), nil
// 	case *SchedulerJob:
// 		objectRequirements := job.jobSchedulingInfo.GetObjectRequirements()
// 		if len(objectRequirements) == 0 {
// 			return nil, errors.New(fmt.Sprintf("no objectRequirements attached to job %s", j.GetId()))
// 		}
// 		return objectRequirements[0].GetPodRequirements(), nil
// 	default:
// 		return nil, errors.New(fmt.Sprintf("could not extract pod spec from type %T", j))
// 	}
// }

// func PodRequirementsFromJob(j LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) (*schedulerobjects.PodRequirements, error) {
// 	switch job := j.(type) {
// 	case *api.Job:
// 		podSpec := util.PodSpecFromJob(job)
// 		return schedulerobjects.PodRequirementsFromPod(&v1.Pod{
// 			ObjectMeta: metav1.ObjectMeta{
// 				Annotations: job.Annotations,
// 			},
// 			Spec: *podSpec,
// 		}, priorityClasses), nil
// 	case *SchedulerJob:
// 		return extractSchedulerRequirements(j, priorityClasses)
// 	default:
// 		return nil, errors.New(fmt.Sprintf("could not extract pod reguirements from type %T", j))
// 	}
// }

// func isNil(j LegacySchedulerJob) (bool, error) {
// 	// if j == nil {
// 	// 	return true, nil
// 	// }
// 	switch job := j.(type) {
// 	case *api.Job:
// 		return job == nil, nil
// 	case *SchedulerJob:
// 		return job == nil, nil
// 	default:
// 		return false, errors.New(fmt.Sprintf("could not determine whether %T is nil", j))
// 	}
// }

func PodRequirementsFromLegacySchedulerJobs[T LegacySchedulerJob](jobs []T, priorityClasses map[string]configuration.PriorityClass) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(jobs))
	for _, job := range jobs {
		info := job.GetRequirements(priorityClasses)
		rv = append(rv, PodRequirementFromJobSchedulingInfo(info))
	}
	return rv
}

func PodRequirementsFromJobSchedulingInfos(infos []*schedulerobjects.JobSchedulingInfo) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(infos))
	for _, info := range infos {
		rv = append(rv, PodRequirementFromJobSchedulingInfo(info))
	}
	return rv
}

func PodRequirementFromJobSchedulingInfo(info *schedulerobjects.JobSchedulingInfo) *schedulerobjects.PodRequirements {
	for _, oreq := range info.ObjectRequirements {
		if preq := oreq.GetPodRequirements(); preq != nil {
			return preq
		}
	}
	// TODO: Don't panic.
	panic("expected at least one pod requirements")
}
