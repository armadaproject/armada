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
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type LegacySchedulerJob interface {
	GetQueue() string
	GetAnnotations() map[string]string
	GetId() string
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
	// Limit- as a fraction of total resources across workers clusters- of resource types at each priority.
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
	// GetQueueJobIds returns the ids of all queued jobs for some queue.
	GetQueueJobIds(queue string) ([]string, error)
	// GetExistingJobsByIds returns any jobs with an id in the provided list.
	GetExistingJobsByIds(jobIds []string) ([]*api.Job, error)
	// TryLeaseJobs tries to create jobs leases and returns the jobs that were successfully leased.
	// Leasing may fail, e.g., if the job was concurrently leased to another executor.
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

// QueuedJobsIterator is an iterator over all jobs in a queue.
// It lazily loads jobs in batches from Redis asynch.
type QueuedJobsIterator struct {
	ctx context.Context
	err error
	c   chan *api.Job
}

func NewQueuedJobsIterator(ctx context.Context, queue string, repo SchedulerJobRepository) (*QueuedJobsIterator, error) {
	batchSize := 16
	g, ctx := errgroup.WithContext(ctx)
	it := &QueuedJobsIterator{
		ctx: ctx,
		c:   make(chan *api.Job, 2*batchSize), // 2x batchSize to load one batch async.
	}

	jobIds, err := repo.GetQueueJobIds(queue)
	if err != nil {
		it.err = err
		return nil, err
	}
	g.Go(func() error { return queuedJobsIteratorLoader(ctx, jobIds, it.c, batchSize, repo) })

	return it, nil
}

func (it *QueuedJobsIterator) Next() (*api.Job, error) {
	// Once this function has returned error,
	// it will return this error on every invocation.
	if it.err != nil {
		return nil, it.err
	}

	// Get one job that was loaded asynchrounsly.
	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err() // Return an error if called again.
		return nil, it.err
	case job, ok := <-it.c:
		if !ok {
			return nil, nil
		}
		return job, nil
	}
}

// queuedJobsIteratorLoader loads jobs from Redis lazily.
// Used with QueuedJobsIterator.
func queuedJobsIteratorLoader(ctx context.Context, jobIds []string, ch chan *api.Job, batchSize int, repo SchedulerJobRepository) error {
	defer close(ch)
	batch := make([]string, batchSize)
	for i, jobId := range jobIds {
		batch[i%len(batch)] = jobId
		if (i+1)%len(batch) == 0 || i == len(jobIds)-1 {
			jobs, err := repo.GetExistingJobsByIds(batch[:i%len(batch)+1])
			if err != nil {
				return err
			}
			for _, job := range jobs {
				if job == nil {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- job:
				}
			}
		}
	}
	return nil
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
	// Jobs in a gang must specify the total number of jsobs in the gang via this annotation.
	gangCardinalityAnnotation string
	// Groups jobs by the gang they belong to.
	jobsByGangId map[string][]T
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
	if it.jobsByGangId == nil {
		it.jobsByGangId = make(map[string][]T)
	}

	// Get one job at a time from the underlying iterator until we either
	// 1. get a job that isn't part of a gang, in which case we yield it immediately, or
	// 2. get the final job in a gang, in which case we yield the entire gang.
	for {
		job, err := it.queuedJobsIterator.Next()
		if err != nil {
			return nil, err
		}
		isNil, err := isNil(job)
		if err != nil {
			return nil, err
		}
		if isNil {
			return nil, nil
		}

		gangId, gangCardinality, isGangJob, err := GangIdAndCardinalityFromAnnotations(job.GetAnnotations(), it.gangIdAnnotation, it.gangCardinalityAnnotation)
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
				return gang, nil
			}
		} else {
			return []T{job}, nil
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
	var consecutiveUnschedulableJobs uint
	for gang, err := it.queuedGangIterator.Next(); gang != nil; gang, err = it.queuedGangIterator.Next() {
		if err != nil {
			return nil, err
		}
		if it.MaxConsecutiveUnschedulableJobs != 0 && consecutiveUnschedulableJobs == it.MaxConsecutiveUnschedulableJobs {
			break
		}
		reports, err := it.schedulingReportsFromJobs(it.ctx, gang)
		if err != nil {
			return nil, err
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
			consecutiveUnschedulableJobs++
		} else {
			return reports, nil
		}
	}
	return nil, nil
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
	timestamp := time.Now()
	for i, job := range jobs {
		jobId, err := uuidFromUlidString(job.GetId())
		if err != nil {
			return nil, err
		}
		req, err := extractSchedulerRequirements(job, it.PriorityClasses)
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
			Resources: common.FromResourceList(req.ResourceRequirements.Requests),
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

// CandidateGangIterator multiplexes between queues.
// Responsible for maintaining fair share and enforcing cross-queue scheduling constraints.
type CandidateGangIterator[T LegacySchedulerJob] struct {
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport[T]
	ctx                   context.Context
	iteratorsByQueue      map[string]*QueueCandidateGangIterator[T]
	// These factors influence the fraction of resources assigned to each queue.
	priorityFactorByQueue map[string]float64
	// Random number generator, used to select queues
	rand *rand.Rand
}

func (it *CandidateGangIterator[T]) Next() ([]*JobSchedulingReport[T], error) {
	if it.MaximumJobsToSchedule != 0 && it.SchedulingRoundReport.NumScheduledJobs == int(it.MaximumJobsToSchedule) {
		it.SchedulingRoundReport.TerminationReason = "maximum number of jobs scheduled"
		return nil, nil
	}

	// Aggregate resource usage by queue.
	// Used to fairly share resources between queues.
	aggregatedResourceUsageByQueue := make(map[string]schedulerobjects.ResourceList)
	for queue := range it.priorityFactorByQueue {
		if report := it.SchedulingRoundReport.QueueSchedulingRoundReports[queue]; report != nil {
			rl := report.InitialResourcesByPriority.AggregateByResource()
			rl.Add(report.ScheduledResourcesByPriority.AggregateByResource())
			aggregatedResourceUsageByQueue[queue] = rl
		}
	}

	// Yield a gang.
	for {
		// First, select which queue to schedule from.
		// Queues below their fair share are selected with higher probability.
		var queue string
		var queueIt *QueueCandidateGangIterator[T]
		for {
			if len(it.priorityFactorByQueue) == 0 {
				// No queued jobs left.
				return nil, nil
			}
			weights := queueSelectionWeights(
				it.priorityFactorByQueue,
				aggregatedResourceUsageByQueue,
				it.ResourceScarcity,
			)
			queue, _ = pickQueueRandomly(weights, it.rand)

			if iter := it.iteratorsByQueue[queue]; iter != nil {
				queueIt = iter
				break
			} else {
				log.Errorf("iterator missing for queue %s", queue)
				delete(it.priorityFactorByQueue, queue)
			}
		}

		// Then, find a gang from that queue that could potentially be scheduled.
		for reports, err := queueIt.Next(); reports != nil; reports, err = queueIt.Next() {
			if err != nil {
				return nil, err
			}

			// Check overall per-round resource limits.
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
			} else {
				return reports, nil
			}
		}

		// No more jobs to process for this queue.
		delete(it.priorityFactorByQueue, queue)
	}
}

func PriorityFromJob(job *api.Job, priorityByPriorityClassName map[string]configuration.PriorityClass) (priority int32, ok bool) {
	return schedulerobjects.PriorityFromPodSpec(util.PodSpecFromJob(job), priorityByPriorityClassName)
}

func uuidFromUlidString(ulid string) (uuid.UUID, error) {
	protoUuid, err := armadaevents.ProtoUuidFromUlidString(ulid)
	if err != nil {
		return uuid.UUID{}, err
	}
	return armadaevents.UuidFromProtoUuid(protoUuid), nil
}

// exceedsResourceLimits returns true if used[t]/total[t] > limits[t] for some resource t,
// and, if that is the case, a string indicating which resource limit was exceeded.
func exceedsResourceLimits(ctx context.Context, used, total schedulerobjects.ResourceList, limits map[string]float64) (bool, string) {
	for resourceType, limit := range limits {
		totalAmount := total.Get(resourceType)
		usedAmount := used.Get(resourceType)
		if common.QuantityAsFloat64(usedAmount)/common.QuantityAsFloat64(totalAmount) > limit {
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

func PodRequirementsFromJobs(priorityClasses map[string]configuration.PriorityClass, jobs []*api.Job) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(jobs))
	for _, job := range jobs {
		rv = append(rv, PodRequirementsFromJob(priorityClasses, job)...)
	}
	return rv
}

func PodRequirementsFromJob(priorityClasses map[string]configuration.PriorityClass, job *api.Job) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, 1+len(job.PodSpecs))
	if job.PodSpec != nil {
		req := schedulerobjects.PodRequirementsFromPod(&v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: job.Annotations,
			},
			Spec: *job.PodSpec,
		}, priorityClasses)
		rv = append(rv, req)
	}
	for _, spec := range job.PodSpecs {
		if spec != nil {
			req := schedulerobjects.PodRequirementsFromPod(&v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: job.Annotations,
				},
				Spec: *spec,
			}, priorityClasses)
			rv = append(rv, req)
		}
	}
	return rv
}

type LegacyScheduler struct {
	ctx context.Context
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport[*api.Job]
	CandidateGangIterator *CandidateGangIterator[*api.Job]
	// Contains all nodes to be considered for scheduling.
	// Used for matching pods with nodes.
	NodeDb *NodeDb
	// Used to request jobs from Redis and to mark jobs as leased.
	JobRepository SchedulerJobRepository
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

func NewLegacyScheduler(
	ctx context.Context,
	constraints SchedulingConstraints,
	config configuration.SchedulingConfig,
	nodeDb *NodeDb,
	jobRepository SchedulerJobRepository,
	priorityFactorByQueue map[string]float64,
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

	schedulingRoundReport := NewSchedulingRoundReport[*api.Job](
		constraints.TotalResources,
		priorityFactorByQueue,
		initialResourcesByQueueAndPriority,
	)

	// Per-queue iterator pipelines.
	iteratorsByQueue := make(map[string]*QueueCandidateGangIterator[*api.Job])
	for queue := range priorityFactorByQueue {
		// Load jobs from Redis.
		queuedJobsIterator, err := NewQueuedJobsIterator(ctx, queue, jobRepository)
		if err != nil {
			return nil, err
		}

		// Group jobs into gangs, to be scheduled together.
		queuedGangIterator := NewQueuedGangIterator[*api.Job](
			ctx,
			queuedJobsIterator,
			config.GangIdAnnotation,
			config.GangCardinalityAnnotation,
		)

		// Enforce per-queue constraints.
		iteratorsByQueue[queue] = &QueueCandidateGangIterator[*api.Job]{
			SchedulingConstraints:      constraints,
			QueueSchedulingRoundReport: schedulingRoundReport.QueueSchedulingRoundReports[queue],
			ctx:                        ctx,
			queuedGangIterator:         queuedGangIterator,
		}
	}

	// Multiplex between queues and enforce cross-queue constraints.
	candidateGangIterator := &CandidateGangIterator[*api.Job]{
		SchedulingConstraints: constraints,
		SchedulingRoundReport: schedulingRoundReport,
		ctx:                   ctx,
		iteratorsByQueue:      iteratorsByQueue,
		priorityFactorByQueue: maps.Clone(priorityFactorByQueue),
	}

	return &LegacyScheduler{
		ctx:                   ctx,
		SchedulingConstraints: constraints,
		SchedulingRoundReport: schedulingRoundReport,
		CandidateGangIterator: candidateGangIterator,
		NodeDb:                nodeDb,
		JobRepository:         jobRepository,
	}, nil
}

func (sched *LegacyScheduler) Schedule() ([]*api.Job, error) {
	log := ctxlogrus.Extract(sched.ctx)
	defer func() {
		sched.SchedulingRoundReport.Finished = time.Now()
	}()

	jobsToLeaseByQueue := make(map[string][]*api.Job, 0)
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

		jobs := make([]*api.Job, len(reports))
		for i, r := range reports {
			jobs[i] = r.Job
		}
		reqs := PodRequirementsFromJobs(sched.PriorityClasses, jobs)

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

	// Try to create leases.
	jobs := make([]*api.Job, 0, numJobsToLease)
	for queue, jobsToLease := range jobsToLeaseByQueue {

		// TryLeaseJobs returns a list of jobs that were successfully leased.
		// For example, jobs concurrently leased to another executor are skipped.
		//
		// TODO: Reports generated above will be incorrect if creating the lease fails.
		successfullyLeasedJobs, err := sched.JobRepository.TryLeaseJobs(sched.ExecutorId, queue, jobsToLease)
		if err != nil {
			logging.WithStacktrace(log, err).Error("failed to lease jobs")
		}
		jobs = append(jobs, successfullyLeasedJobs...)
	}
	return jobs, nil
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
		usage += common.QuantityAsFloat64(quantity) * scarcity
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

func extractSchedulerRequirements(j LegacySchedulerJob, pcs map[string]configuration.PriorityClass) (*schedulerobjects.PodRequirements, error) {
	switch job := j.(type) {
	case *api.Job:
		podSpec := util.PodSpecFromJob(job)
		if podSpec == nil {
			return nil, errors.New("failed to get pod spec")
		}
		return schedulerobjects.PodRequirementsFromPodSpec(
			podSpec,
			pcs,
		), nil
	default:
		return nil, errors.New(fmt.Sprintf("could not extract pod spec from type %T", j))
	}
}

func isNil(j LegacySchedulerJob) (bool, error) {
	switch job := j.(type) {
	case *api.Job:
		return job == nil, nil
	default:
		return false, errors.New(fmt.Sprintf("could not determine whether %T is nil", j))
	}
}
