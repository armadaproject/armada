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

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

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

// QueueCandidateJobsIterator is an iterator over jobs in a queue that could be scheduled.
// Responsible for enforcing cross-queue scheduling constraints, e.g.,
// - would not exceed per-round resource limits and
// - would not exceed total per-queue resource limits.
type QueueCandidateJobsIterator struct {
	SchedulingConstraints
	QueueSchedulingRoundReport *QueueSchedulingRoundReport
	ctx                        context.Context
	err                        error
	// Iterator over all jobs in this queue.
	jobsIterator *QueuedJobsIterator
}

func (it *QueueCandidateJobsIterator) Next() (*JobSchedulingReport, error) {
	// Once this function has returned error,
	// it will return this error on every invocation.
	if it.err != nil {
		return nil, it.err
	}

	// Return the next job in the queue that could potentially be scheduled.
	var consecutiveUnschedulableJobs uint
	for job, err := it.jobsIterator.Next(); job != nil; job, err = it.jobsIterator.Next() {
		if err != nil {
			return nil, err
		}
		if it.MaxConsecutiveUnschedulableJobs != 0 && consecutiveUnschedulableJobs == it.MaxConsecutiveUnschedulableJobs {
			break
		}
		jobSchedulingReport, err := it.schedulingReportFromJob(it.ctx, job)
		if err != nil {
			return nil, err
		}
		if jobSchedulingReport.UnschedulableReason != "" {
			it.QueueSchedulingRoundReport.AddJobSchedulingReport(jobSchedulingReport)
			consecutiveUnschedulableJobs++
			continue
		}
		return jobSchedulingReport, nil
	}
	return nil, nil
}

func (it *QueueCandidateJobsIterator) schedulingReportFromJob(ctx context.Context, job *api.Job) (*JobSchedulingReport, error) {
	// Create a scheduling report for this job.
	jobId, err := uuidFromUlidString(job.Id)
	if err != nil {
		return nil, err
	}
	podSpec := util.PodSpecFromJob(job)
	if podSpec == nil {
		return nil, errors.New("failed to get pod spec")
	}
	req := schedulerobjects.PodRequirementsFromPodSpec(
		podSpec,
		it.PriorityClasses,
	)
	jobSchedulingReport := &JobSchedulingReport{
		Timestamp:  time.Now(),
		JobId:      jobId,
		Job:        job,
		Req:        req,
		ExecutorId: it.ExecutorId,
	}

	// Priority and total requests for this job.
	priority, _ := PriorityFromJob(job, it.PriorityClasses)
	jobTotalResourceRequests := schedulerobjects.ResourceList{
		Resources: common.TotalJobResourceRequest(job),
	}

	// Check that the job is large enough for this executor.
	if ok, reason := jobIsLargeEnough(jobTotalResourceRequests, it.MinimumJobSize); !ok {
		jobSchedulingReport.UnschedulableReason = reason
		return jobSchedulingReport, nil
	}

	// MaximalResourceFractionToSchedulePerQueue check.
	roundQueueResourcesByPriority := it.QueueSchedulingRoundReport.ScheduledResourcesByPriority.DeepCopy()
	roundQueueResourcesByPriority.AddResouceList(priority, jobTotalResourceRequests)
	if exceeded, reason := exceedsResourceLimits(
		ctx,
		roundQueueResourcesByPriority.AggregateByResource(),
		it.SchedulingConstraints.TotalResources,
		it.MaximalResourceFractionToSchedulePerQueue,
	); exceeded {
		jobSchedulingReport.UnschedulableReason = reason + " (per scheduling round limit for this queue)"
		return jobSchedulingReport, nil
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
		jobSchedulingReport.UnschedulableReason = reason + " (total limit for this queue)"
		return jobSchedulingReport, nil
	}

	// MaximalCumulativeResourceFractionPerQueueAndPriority check.
	if exceeded, reason := exceedsPerPriorityResourceLimits(
		ctx,
		priority,
		totalQueueResourcesByPriority,
		it.SchedulingConstraints.TotalResources,
		it.MaximalCumulativeResourceFractionPerQueueAndPriority,
	); exceeded {
		jobSchedulingReport.UnschedulableReason = reason + " (total limit for this queue)"
		return jobSchedulingReport, nil
	}

	return jobSchedulingReport, nil
}

// CandidateJobsIterator multiplexes jobs between queues.
// Responsible for enforcing cross-queue scheduling constraints.
type CandidateJobsIterator struct {
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport
	ctx                   context.Context
	err                   error
	iteratorsByQueue      map[string]*QueueCandidateJobsIterator
	// These factors influence the fraction of resources assigned to each queue.
	priorityFactorByQueue map[string]float64
	// Random number generator, used to select queues
	rand *rand.Rand
}

func (it *CandidateJobsIterator) Next() (*JobSchedulingReport, error) {
	// Once this function has returned error,
	// it will return this error on every invocation.
	if it.err != nil {
		return nil, it.err
	}

	// TODO: Use uint always.
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

	// Yield a job.
	for {
		// First, select which queue to schedule from.
		// Queues below their fair share are selected with higher probability.
		var queue string
		var queueIt *QueueCandidateJobsIterator
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

		// Then, find a job from that queue that could potentially be scheduled.
		for report, err := queueIt.Next(); report != nil; report, err = queueIt.Next() {
			if err != nil {
				return nil, err
			}

			// Check overall per-round resource limits.
			totalScheduledResources := it.SchedulingRoundReport.ScheduledResourcesByPriority.AggregateByResource()
			jobResourceRequests := schedulerobjects.ResourceListFromV1ResourceList(report.Req.ResourceRequirements.Requests)
			totalScheduledResources.Add(jobResourceRequests)
			if exceeded, reason := exceedsResourceLimits(
				it.ctx,
				totalScheduledResources,
				it.SchedulingConstraints.TotalResources,
				it.MaximalResourceFractionToSchedule,
			); exceeded {
				report.UnschedulableReason = reason + " (overall per scheduling round limit)"
				it.SchedulingRoundReport.AddJobSchedulingReport(report)
				continue
			}

			return report, nil
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

type CandidateGangIterator struct {
	ctx context.Context
	CandidateJobsIterator
	// Jobs are grouped into gangs by this annotation.
	gangIdAnnotation string
	// Jobs in a gang specify the number of jobs in the gang via this annotation.
	gangCardinalityAnnotation string
	// Map from queue to the gangs for that queue.
	// Each gang is made up of a slice of jobs with equal value for gangIdAnnotation.
	gangsByQueue map[string]map[string][]*JobSchedulingReport
}

func NewQueueCandidateGangIterator(ctx context.Context, it *CandidateJobsIterator, gangIdAnnotation, gangCardinalityAnnotation string) *CandidateGangIterator {
	return &CandidateGangIterator{
		ctx:                       ctx,
		CandidateJobsIterator:     *it,
		gangIdAnnotation:          gangIdAnnotation,
		gangCardinalityAnnotation: gangCardinalityAnnotation,
		gangsByQueue:              make(map[string]map[string][]*JobSchedulingReport),
	}
}

func (it *CandidateGangIterator) Next() ([]*JobSchedulingReport, error) {
	// Once this function has returned error,
	// it will return this error on every invocation.
	if it.err != nil {
		return nil, it.err
	}

	// Get one job at a time from the underlying iterator until we either
	// 1. get a job that isn't part of a gang, in which case we yield it immediately, or
	// 2. get the final job in a gang, in which case we yield the entire gang.
	for {
		report, err := it.CandidateJobsIterator.Next()
		if err != nil {
			return nil, err
		}
		if report == nil {
			return nil, nil
		}

		gangId, gangCardinality, isGangJob, err := GangIdAndCardinalityFromJob(report.Job, it.gangIdAnnotation, it.gangCardinalityAnnotation)
		if err != nil {
			log := ctxlogrus.Extract(it.ctx)
			logging.WithStacktrace(log, err).Errorf("failed to get gang cardinality for job %s", report.Job.Id)
			gangCardinality = 1 // Schedule jobs with invalid gang cardinality one by one.
		}
		if isGangJob {
			jobsByGangId, ok := it.gangsByQueue[report.Job.Queue]
			if !ok {
				jobsByGangId = make(map[string][]*JobSchedulingReport)
				it.gangsByQueue[report.Job.Queue] = jobsByGangId
			}

			jobsByGangId[gangId] = append(jobsByGangId[gangId], report)
			gang := jobsByGangId[gangId]
			if len(gang) == gangCardinality {
				delete(jobsByGangId, gangId)
				return gang, nil
			}
		} else {
			return []*JobSchedulingReport{report}, nil
		}
	}
}

func (sched *LegacyScheduler) podRequirementsFromJobs(jobs []*api.Job) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, len(jobs))
	for _, job := range jobs {
		rv = append(rv, sched.podRequirementsFromJob(job)...)
	}
	return rv
}

func (sched *LegacyScheduler) podRequirementsFromJob(job *api.Job) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, 0, 1+len(job.PodSpecs))
	rv = append(rv, schedulerobjects.PodRequirementsFromPodSpec(job.PodSpec, sched.PriorityClasses))
	for _, podSpec := range job.PodSpecs {
		req := schedulerobjects.PodRequirementsFromPodSpec(podSpec, sched.PriorityClasses)
		rv = append(rv, req)
	}
	return rv
}

type LegacyScheduler struct {
	ctx context.Context
	SchedulingConstraints
	SchedulingRoundReport *SchedulingRoundReport
	CandidateGangIterator *CandidateGangIterator
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

	schedulingRoundReport := NewSchedulingRoundReport(
		constraints.TotalResources,
		priorityFactorByQueue,
		initialResourcesByQueueAndPriority,
	)

	iteratorsByQueue := make(map[string]*QueueCandidateJobsIterator)
	for queue := range priorityFactorByQueue {
		queuedJobsIterator, err := NewQueuedJobsIterator(ctx, queue, jobRepository)
		if err != nil {
			return nil, err
		}
		iteratorsByQueue[queue] = &QueueCandidateJobsIterator{
			SchedulingConstraints:      constraints,
			QueueSchedulingRoundReport: schedulingRoundReport.QueueSchedulingRoundReports[queue],
			ctx:                        ctx,
			jobsIterator:               queuedJobsIterator,
		}
	}

	candidateJobsIterator := &CandidateJobsIterator{
		SchedulingConstraints: constraints,
		SchedulingRoundReport: schedulingRoundReport,
		ctx:                   ctx,
		iteratorsByQueue:      iteratorsByQueue,
		priorityFactorByQueue: maps.Clone(priorityFactorByQueue),
	}
	candidateGangIterator := NewQueueCandidateGangIterator(
		ctx,
		candidateJobsIterator,
		config.GangIdAnnotation,
		config.GangCardinalityAnnotation,
	)

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
		reqs := sched.podRequirementsFromJobs(jobs)

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
					r.UnschedulableReason = "at least one pod in the gang did not fit on any node"
				}
			} else {
				for _, r := range reports {
					r.UnschedulableReason = "pod does not fit on any node"
				}
			}
			for _, r := range reports {
				sched.SchedulingRoundReport.AddJobSchedulingReport(r)
			}
		} else {
			for _, r := range reports {
				jobsToLeaseByQueue[r.Job.Queue] = append(jobsToLeaseByQueue[r.Job.Queue], r.Job)
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

func GangIdAndCardinalityFromJob(job *api.Job, gangIdAnnotation, gangCardinalityAnnotation string) (string, int, bool, error) {
	return GangIdAndCardinalityFromAnnotations(job.Annotations, gangIdAnnotation, gangCardinalityAnnotation)
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
	if gangCardinality == 0 {
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
	pick := sum * random.Float64()

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
