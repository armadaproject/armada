package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
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
type SchedulerJobRepository interface {
	// GetJobIterator returns a iterator over queued jobs for a given queue.
	GetJobIterator(ctx context.Context, queue string) (JobIterator, error)
}

type JobIterator interface {
	Next() (LegacySchedulerJob, error)
}

// QueuedGangIterator is an iterator over all gangs in a queue,
// where a gang is a set of jobs for which the gangIdAnnotation has equal value.
// A gang is yielded once the final member of the gang has been received.
// Jobs without gangIdAnnotation are considered to be gangs of cardinality 1.
type QueuedGangIterator struct {
	ctx                context.Context
	queuedJobsIterator JobIterator
	// Jobs are grouped into gangs by this annotation.
	gangIdAnnotation string
	// Jobs in a gang must specify the total number of jobs in the gang via this annotation.
	gangCardinalityAnnotation string
	// Groups jobs by the gang they belong to.
	jobsByGangId map[string][]LegacySchedulerJob
	next         []LegacySchedulerJob
}

func NewQueuedGangIterator(ctx context.Context, it JobIterator, gangIdAnnotation, gangCardinalityAnnotation string) *QueuedGangIterator {
	return &QueuedGangIterator{
		ctx:                       ctx,
		queuedJobsIterator:        it,
		gangIdAnnotation:          gangIdAnnotation,
		gangCardinalityAnnotation: gangCardinalityAnnotation,
		jobsByGangId:              make(map[string][]LegacySchedulerJob),
	}
}

func (it *QueuedGangIterator) Next() ([]LegacySchedulerJob, error) {
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

func (it *QueuedGangIterator) Peek() ([]LegacySchedulerJob, error) {
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
			it.next = []LegacySchedulerJob{job}
			return it.next, nil
		}
	}
}

// QueueCandidateGangIterator is an iterator over gangs in a queue that could be scheduled
// without exceeding per-queue limits.
type QueueCandidateGangIterator struct {
	ctx context.Context
	SchedulingConstraints
	QueueSchedulingRoundReport *QueueSchedulingRoundReport
	queuedGangIterator         *QueuedGangIterator
}

func (it *QueueCandidateGangIterator) Next() ([]*JobSchedulingReport, error) {
	if v, err := it.Peek(); err != nil {
		return nil, err
	} else {
		if err := it.Clear(); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func (it *QueueCandidateGangIterator) Clear() error {
	if err := it.queuedGangIterator.Clear(); err != nil {
		return err
	}
	return nil
}

func (it *QueueCandidateGangIterator) Peek() ([]*JobSchedulingReport, error) {
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

func (it *QueueCandidateGangIterator) f(gang []LegacySchedulerJob) ([]*JobSchedulingReport, bool, error) {
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

func (it *QueueCandidateGangIterator) schedulingReportsFromJobs(ctx context.Context, jobs []LegacySchedulerJob) ([]*JobSchedulingReport, error) {
	if jobs == nil {
		return nil, nil
	}
	if len(jobs) == 0 {
		return make([]*JobSchedulingReport, 0), nil
	}

	// Create the scheduling reports and calculate the total requests of the gang
	// We consider the total resource requests of a gang
	// to be the sum of the requests over all jobs in the gang.
	reports := make([]*JobSchedulingReport, len(jobs))
	gangTotalResourceRequests := totalResourceRequestsFromJobs(jobs, it.PriorityClasses)
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
		reports[i] = &JobSchedulingReport{
			Timestamp:  timestamp,
			JobId:      jobId,
			Job:        job,
			Req:        req,
			ExecutorId: it.ExecutorId,
		}
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

func totalResourceRequestsFromJobs(jobs []LegacySchedulerJob, priorityClasses map[string]configuration.PriorityClass) schedulerobjects.ResourceList {
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
type QueueCandidateGangIteratorPQ []*QueueCandidateGangIteratorItem

type QueueCandidateGangIteratorItem struct {
	// Each item corresponds to a queue.
	queue string
	// Iterator for this queue.
	it *QueueCandidateGangIterator
	// Most recent value produced by the iterator.
	// Cached here to avoid repeating scheduling checks unnecessarily.
	v []*JobSchedulingReport
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

// CandidateGangIterator multiplexes between queues.
// Responsible for maintaining fair share and enforcing cross-queue scheduling constraints.
type CandidateGangIterator struct {
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport
	ctx                   context.Context
	// These factors influence the fraction of resources assigned to each queue.
	priorityFactorByQueue map[string]float64
	// For each queue, weight is the inverse of the priority factor.
	weightByQueue map[string]float64
	// Sum of all weights.
	weightSum float64
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq QueueCandidateGangIteratorPQ
}

func NewCandidateGangIterator(
	schedulingConstraints SchedulingConstraints,
	schedulingRoundReport *SchedulingRoundReport,
	ctx context.Context,
	iteratorsByQueue map[string]*QueueCandidateGangIterator,
	priorityFactorByQueue map[string]float64,
) (*CandidateGangIterator, error) {
	if len(iteratorsByQueue) != len(priorityFactorByQueue) {
		return nil, errors.Errorf("iteratorsByQueue and priorityFactorByQueue are not of equal length")
	}
	weightSum := 0.0
	weightByQueue := make(map[string]float64)
	for queue, priorityFactor := range priorityFactorByQueue {
		if _, ok := iteratorsByQueue[queue]; !ok {
			return nil, errors.Errorf("no iterator found for queue %s", queue)
		}
		weight := 1 / math.Max(priorityFactor, 1)
		weightByQueue[queue] = weight
		weightSum += weight
	}
	rv := &CandidateGangIterator{
		SchedulingConstraints: schedulingConstraints,
		SchedulingRoundReport: schedulingRoundReport,
		ctx:                   ctx,
		priorityFactorByQueue: priorityFactorByQueue,
		weightByQueue:         weightByQueue,
		weightSum:             weightSum,
		pq:                    make(QueueCandidateGangIteratorPQ, 0, len(iteratorsByQueue)),
	}
	for queue, queueIt := range iteratorsByQueue {
		if err := rv.pushToPQ(queue, queueIt); err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func (it *CandidateGangIterator) pushToPQ(queue string, queueIt *QueueCandidateGangIterator) error {
	reports, err := queueIt.Peek()
	if err != nil {
		return err
	}
	if reports == nil {
		return nil
	}
	gang := make([]LegacySchedulerJob, len(reports))
	for i, report := range reports {
		gang[i] = report.Job
	}
	initialResourcesForQueue := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue].InitialResourcesByPriority
	scheduledResourcesForQueue := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue].ScheduledResourcesByPriority
	totalResourcesForQueue := initialResourcesForQueue.DeepCopy()
	totalResourcesForQueue.Add(scheduledResourcesForQueue)
	totalResourcesForQueueWithGang := totalResourcesForQueue.AggregateByResource()
	totalResourcesForQueueWithGang.Add(totalResourceRequestsFromJobs(gang, it.PriorityClasses))
	fairShare := it.weightByQueue[queue] / it.weightSum
	used := ResourceListAsWeightedApproximateFloat64(it.ResourceScarcity, totalResourcesForQueueWithGang)
	total := math.Max(ResourceListAsWeightedApproximateFloat64(it.ResourceScarcity, it.TotalResources), 1)
	fractionOfFairShare := (used / total) / fairShare
	item := &QueueCandidateGangIteratorItem{
		queue:               queue,
		it:                  queueIt,
		v:                   reports,
		fractionOfFairShare: fractionOfFairShare,
	}
	heap.Push(&it.pq, item)
	return nil
}

func (it *CandidateGangIterator) Next() ([]*JobSchedulingReport, error) {
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

func (it *CandidateGangIterator) Peek() ([]*JobSchedulingReport, error) {
	if it.MaximumJobsToSchedule != 0 && it.SchedulingRoundReport.NumScheduledJobs == int(it.MaximumJobsToSchedule) {
		it.SchedulingRoundReport.TerminationReason = "maximum number of jobs scheduled"
		return nil, nil
	}

	// Yield a gang.
	// To ensure the schedulability constraints are still valid,
	// pop and push items from/to the pq until we've popped the same item twice consecutively,
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
		reports := item.v // Cached value is guaranteed to be fresh here.
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

func (it *CandidateGangIterator) f(reports []*JobSchedulingReport) ([]*JobSchedulingReport, bool, error) {
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

func uuidFromUlidString(ulid string) (uuid.UUID, error) {
	protoUuid, err := armadaevents.ProtoUuidFromUlidString(ulid)
	if err != nil {
		return uuid.UUID{}, err
	}
	return armadaevents.UuidFromProtoUuid(protoUuid), nil
}

// exceedsResourceLimits returns true if used/total > limits for some resource t,
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

type LegacyScheduler struct {
	ctx context.Context
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport
	CandidateGangIterator *CandidateGangIterator
	// Contains all nodes to be considered for scheduling.
	// Used for matching pods with nodes.
	NodeDb *NodeDb
	// Jobs are grouped into gangs by this annotation.
	GangIdAnnotation string
	// Jobs in a gang specify the number of jobs in the gang via this annotation.
	GangCardinalityAnnotation string
}

func (sched *LegacyScheduler) String() string {
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

type Queue struct {
	name           string
	priorityFactor float64
	jobIterator    JobIterator
}

func NewQueue(name string, priorityFactor float64, jobIterator JobIterator) (*Queue, error) {
	if priorityFactor <= 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "priorityFactor",
			Value:   priorityFactor,
			Message: "priorityFactor must be positive",
		})
	}
	return &Queue{
		name:           name,
		priorityFactor: priorityFactor,
		jobIterator:    jobIterator,
	}, nil
}

func NewLegacyScheduler(
	ctx context.Context,
	constraints SchedulingConstraints,
	config configuration.SchedulingConfig,
	nodeDb *NodeDb,
	queues []*Queue,
	initialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) (*LegacyScheduler, error) {
	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, constraints.TotalResources) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		return nil, errors.New("no resources with non-zero weight available for scheduling")
	}
	if ResourceListAsWeightedApproximateFloat64(constraints.ResourceScarcity, nodeDb.totalResources) == 0 {
		// This refers to the resources currently considered for schedling.
		return nil, errors.New("no resources with non-zero weight available for scheduling in NodeDb")
	}

	priorityFactorByQueue := make(map[string]float64)
	for _, queue := range queues {
		priorityFactorByQueue[queue.name] = queue.priorityFactor
	}
	schedulingRoundReport := NewSchedulingRoundReport(
		constraints.TotalResources,
		priorityFactorByQueue,
		initialResourcesByQueueAndPriority,
	)

	// Per-queue iterator pipelines.
	gangIteratorsByQueue := make(map[string]*QueueCandidateGangIterator)
	for _, queue := range queues {
		// Group jobs into gangs, to be scheduled together.
		queuedGangIterator := NewQueuedGangIterator(
			ctx,
			queue.jobIterator,
			config.GangIdAnnotation,
			config.GangCardinalityAnnotation,
		)

		// Enforce per-queue constraints.
		gangIteratorsByQueue[queue.name] = &QueueCandidateGangIterator{
			SchedulingConstraints:      constraints,
			QueueSchedulingRoundReport: schedulingRoundReport.QueueSchedulingRoundReports[queue.name],
			ctx:                        ctx,
			queuedGangIterator:         queuedGangIterator,
		}
	}

	// Multiplex between queues and enforce cross-queue constraints.
	candidateGangIterator, err := NewCandidateGangIterator(
		constraints,
		schedulingRoundReport,
		ctx,
		gangIteratorsByQueue,
		maps.Clone(priorityFactorByQueue),
	)
	if err != nil {
		return nil, err
	}

	return &LegacyScheduler{
		ctx:                   ctx,
		SchedulingConstraints: constraints,
		SchedulingRoundReport: schedulingRoundReport,
		CandidateGangIterator: candidateGangIterator,
		NodeDb:                nodeDb,
	}, nil
}

func (sched *LegacyScheduler) Schedule() ([]LegacySchedulerJob, error) {
	defer func() {
		sched.SchedulingRoundReport.Finished = time.Now()
	}()

	jobsToLeaseByQueue := make(map[string][]LegacySchedulerJob, 0)
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

		jobs := make([]LegacySchedulerJob, len(reports))
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
	rv := make([]LegacySchedulerJob, 0)
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
	if podReqs == nil {
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

func ResourceListAsWeightedApproximateFloat64(resourceScarcity map[string]float64, rl schedulerobjects.ResourceList) float64 {
	usage := 0.0
	for resourceName, quantity := range rl.Resources {
		scarcity := resourceScarcity[resourceName]
		usage += armadaresource.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

func PodRequirementsFromLegacySchedulerJobs[S ~[]E, E LegacySchedulerJob](jobs S, priorityClasses map[string]configuration.PriorityClass) []*schedulerobjects.PodRequirements {
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
