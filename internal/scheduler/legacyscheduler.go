package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type SchedulerJobRepository interface {
	// GetQueueJobIds returns the ids of all queued jobs for some queue.
	GetQueueJobIds(queue string) ([]string, error)
	// GetExistingJobsByIds returns any jobs with an id in the provided list.
	GetExistingJobsByIds(jobIds []string) ([]*api.Job, error)
	// TryLeaseJobs tries to create jobs leases and returns the jobs that were successfully leased.
	// Leasing may fail, e.g., if the job was concurrently leased to another executor.
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

type JobsIterator interface {
	// Returns the next job, or nil if there are no more jobs.
	Next() (*api.Job, error)
}

// QueuedJobsIterator is an iterator over all jobs in a queue.
// It loads jobs in batches from Redis asynch.
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

// queuedJobsIteratorLoader loads jobs from Redis. Used with QueuedJobsIterator.
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

// QueueCandidateJobsIterator is an iterator over all jobs in a queue
// that could potentially be scheduled. Specifically, all jobs that
// - would not exceed per-round resource limits,
// - would not exceed total per-queue resource limits, and
// - for which there existed at the time of checking a node the job could be scheduled on.
//
// Because other jobs may have been scheduled between this iterator finding a node
// for a job and the main scheduler thread considering that job,
// the main scheduling thread must verify that the job can still be assigned to this node.
type QueueCandidateJobsIterator struct {
	LegacyScheduler
	ctx context.Context
	err error
	// Iterator over all jobs in this queue.
	jobsIterator JobsIterator
	// Total resources assigned to this queue across all clusters.
	totalQueueResources schedulerobjects.ResourceList
	// Total resources assigned to this queue across all clusters by priority.
	totalQueueResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this invocation of the scheduler.
	roundQueueResources schedulerobjects.ResourceList
	// Store reports for each scheduling attempt.
	schedulingReportsRepository *SchedulingReportsRepository
}

func NewQueueCandidateJobsIterator(
	ctx context.Context,
	queue string, initialTotalQueueResources schedulerobjects.ResourceList, initialTotalQueueResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType, scheduler LegacyScheduler,
) (*QueueCandidateJobsIterator, error) {
	jobsIterator, err := NewQueuedJobsIterator(ctx, queue, scheduler.JobRepository)
	if err != nil {
		return nil, err
	}
	return &QueueCandidateJobsIterator{
		LegacyScheduler:               scheduler,
		ctx:                           ctx,
		jobsIterator:                  jobsIterator,
		totalQueueResources:           initialTotalQueueResources.DeepCopy(),
		totalQueueResourcesByPriority: initialTotalQueueResourcesByPriority.DeepCopy(),
		roundQueueResources:           schedulerobjects.ResourceList{},
	}, nil
}

// Update the internal state of the iterator to reflect that a job was leased.
func (it *QueueCandidateJobsIterator) Lease(jobSchedulingReport *JobSchedulingReport) {
	it.totalQueueResources = jobSchedulingReport.TotalQueueResources
	it.roundQueueResources = jobSchedulingReport.RoundQueueResources
	it.totalQueueResourcesByPriority = jobSchedulingReport.TotalQueueResourcesByPriority
	if it.NodeDb != nil {
		for _, report := range jobSchedulingReport.PodSchedulingReports {
			it.NodeDb.BindNodeToPod(jobSchedulingReport.JobId, report.Req, report.Node)
		}
	}
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
		if it.SchedulingConfig.QueueLeaseBatchSize != 0 && consecutiveUnschedulableJobs == it.SchedulingConfig.QueueLeaseBatchSize {
			break
		}
		jobSchedulingReport, err := it.schedulingReportFromJob(it.ctx, job)
		if err != nil {
			return nil, err
		}
		if jobSchedulingReport.UnschedulableReason != "" {
			// Store reports for unsuccessful attempts.
			// Successful attempts are stored by the main scheduling loop.
			// Zero out the job spec to reduce memory usage.
			jobSchedulingReport.Job = nil
			if it.schedulingReportsRepository != nil {
				it.schedulingReportsRepository.Add(job.Queue, jobSchedulingReport)
			}
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
	jobSchedulingReport := &JobSchedulingReport{
		Timestamp:  time.Now(),
		JobId:      jobId,
		Job:        job,
		ExecutorId: it.ExecutorId,
	}

	// Add the resource requests of this job to the total usage for this queue.
	// We mutate copies of it.roundQueueResources and it.totalQueueResources.
	// Later, if the job is scheduled, we update it.round... and it.total in-place.
	//
	// TODO: Account for resource usage separately by priority.
	jobTotalResourceRequests := common.TotalJobResourceRequest(job)
	roundQueueResources := it.roundQueueResources.DeepCopy()
	totalQueueResources := it.totalQueueResources.DeepCopy()
	totalQueueResourcesByPriority := it.totalQueueResourcesByPriority.DeepCopy()
	jobSchedulingReport.RoundQueueResources = roundQueueResources
	jobSchedulingReport.TotalQueueResources = totalQueueResources
	jobSchedulingReport.TotalQueueResourcesByPriority = totalQueueResourcesByPriority
	for resourceType, quantity := range jobTotalResourceRequests {
		q := totalQueueResources.Resources[resourceType]
		q.Add(quantity)
		totalQueueResources.Resources[resourceType] = q

		q = roundQueueResources.Resources[resourceType]
		q.Add(quantity)
		roundQueueResources.Resources[resourceType] = q

		priority, _ := PriorityFromJob(job, it.SchedulingConfig.Preemption.PriorityClasses)
		rl := totalQueueResourcesByPriority[priority]
		if rl.Resources == nil {
			rl.Resources = make(map[string]resource.Quantity)
		}
		q = rl.Resources[resourceType]
		q.Add(quantity)
		rl.Resources[resourceType] = q
		totalQueueResourcesByPriority[priority] = rl
	}

	// Check that the job is large enough for this executor.
	if ok, reason := it.jobIsLargeEnough(jobTotalResourceRequests); !ok {
		jobSchedulingReport.UnschedulableReason = reason
		return jobSchedulingReport, nil
	}

	// Check total per-queue resource limits.
	if exceeded, reason := it.exceedsResourceLimits(
		ctx,
		totalQueueResources,
		it.SchedulingConfig.MaximalResourceFractionPerQueue,
	); exceeded {
		jobSchedulingReport.UnschedulableReason = reason + " (total limit for this queue)"
		return jobSchedulingReport, nil
	}

	// Check per-round resource limits for this queue.
	if exceeded, reason := it.exceedsResourceLimits(
		ctx,
		roundQueueResources,
		it.SchedulingConfig.MaximalResourceFractionToSchedulePerQueue,
	); exceeded {
		jobSchedulingReport.UnschedulableReason = reason + " (per scheduling round limit for this queue)"
		return jobSchedulingReport, nil
	}

	// If a NodeDb is provided, try to find a node on which this job can be scheduled.
	if it.NodeDb != nil {
		podReport, err := it.selectNodeForPod(ctx, jobId, job)
		if err != nil {
			jobSchedulingReport.UnschedulableReason = err.Error()
			return nil, err
		}
		jobSchedulingReport.PodSchedulingReports = append(jobSchedulingReport.PodSchedulingReports, podReport)
		if podReport.Node == nil {
			jobSchedulingReport.UnschedulableReason = "pod does not fit on any node"
			return jobSchedulingReport, nil
		}
	}

	return jobSchedulingReport, nil
}

func PriorityFromJob(job *api.Job, priorityByPriorityClassName map[string]int32) (priority int32, ok bool) {
	return PriorityFromPodSpec(podSpecFromJob(job), priorityByPriorityClassName)
}

// PriorityFromPodSpec returns the priority set in a pod spec.
// If priority is set diectly, that value is returned.
// Otherwise, it returns priorityByPriorityClassName[podSpec.PriorityClassName].
// ok is false if no priority is set for this pod spec, in which case priority is 0.
func PriorityFromPodSpec(podSpec *v1.PodSpec, priorityByPriorityClassName map[string]int32) (priority int32, ok bool) {
	if podSpec == nil {
		return
	}
	if podSpec.Priority != nil {
		priority = *podSpec.Priority
		ok = true
	} else if priorityByPriorityClassName != nil {
		priority, ok = priorityByPriorityClassName[podSpec.PriorityClassName]
	}
	return
}

func uuidFromUlidString(ulid string) (uuid.UUID, error) {
	protoUuid, err := armadaevents.ProtoUuidFromUlidString(ulid)
	if err != nil {
		return uuid.UUID{}, err
	}
	return armadaevents.UuidFromProtoUuid(protoUuid), nil
}

// Check if scheduling this job would exceed per-queue resource limits.
func (scheduler *LegacyScheduler) exceedsResourceLimits(ctx context.Context, rl schedulerobjects.ResourceList, limits map[string]float64) (bool, string) {
	for resourceType, limit := range limits {
		totalAmount := scheduler.TotalResources.Resources[resourceType]
		amountUsedByQueue := rl.Resources[resourceType]
		if amountUsedByQueue.AsApproximateFloat64()/totalAmount.AsApproximateFloat64() > limit {
			return true, fmt.Sprintf("scheduling would exceed %s quota", resourceType)
		}
	}
	return false, ""
}

// Check that this job is at least equal to the minimum job size.
func (scheduler *LegacyScheduler) jobIsLargeEnough(jobTotalResourceRequests common.ComputeResources) (bool, string) {
	// TODO: These per-job checks could be expressed as filter functions, e.g., of type
	// jobsFilterFunc func(*api.Job) bool
	if len(scheduler.MinimumJobSize) == 0 {
		return true, ""
	}
	for resourceType, quantity := range jobTotalResourceRequests {
		if limit, ok := scheduler.MinimumJobSize[resourceType]; ok {
			if limit.Cmp(quantity) == 1 {
				return false, fmt.Sprintf(
					"job requests %s %s, but the minimum is %s",
					quantity.String(), resourceType, limit.String(),
				)
			}
		}
	}
	return true, ""
}

func (scheduler *LegacyScheduler) selectNodeForPod(ctx context.Context, jobId uuid.UUID, job *api.Job) (*PodSchedulingReport, error) {
	podSpec := podSpecFromJob(job)
	if podSpec == nil {
		return nil, errors.New("failed to get pod spec")
	}

	// Try to find a node for this pod.
	// Store the report returned by the NodeDb.
	req := schedulerobjects.PodRequirementsFromPodSpec(podSpec)
	report, err := scheduler.NodeDb.SelectNodeForPod(jobId, req)
	if err != nil {
		return nil, err
	}
	return report, nil
}

type LegacyScheduler struct {
	SchedulingConfig configuration.SchedulingConfig
	// Executor for which we're currently scheduling jobs.
	ExecutorId string
	// Total resources across all clusters.
	// Used when computing resource limits.
	TotalResources schedulerobjects.ResourceList
	// Contains all nodes to be considered for scheduling.
	// Used for matching pods with nodes.
	NodeDb *NodeDb
	// Used to request jobs from Redis and to mark jobs as leased.
	// TODO: Remove. Not needed for updated implementation.
	JobQueue scheduling.JobQueue
	// Used to get gets.
	JobRepository SchedulerJobRepository
	// Minimum quantity allowed for jobs leased to this cluster.
	MinimumJobSize map[string]resource.Quantity
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactorByQueue map[string]float64
	// Random number generator, used to select queues
	Rand *rand.Rand
	// Store reports for each scheduling attempt.
	SchedulingReportsRepository *SchedulingReportsRepository
}

func NewLegacyScheduler(
	schedulingConfig configuration.SchedulingConfig,
	executorId string,
	totalResources schedulerobjects.ResourceList,
	nodes []*schedulerobjects.Node,
	jobRepository SchedulerJobRepository,
	priorityFactorByQueue map[string]float64,
) (*LegacyScheduler, error) {
	priorities := make([]int32, 0)
	for _, priority := range schedulingConfig.Preemption.PriorityClasses {
		priorities = append(priorities, priority)
	}
	if len(priorities) == 0 {
		priorities = []int32{0}
	}

	resources := schedulingConfig.IndexedResources
	if len(resources) == 0 {
		resources = []string{"cpu", "memory"}
	}

	nodeDb, err := NewNodeDb(priorities, resources)
	if err != nil {
		return nil, err
	}

	if len(totalResources.Resources) == 0 {
		return nil, errors.New("no resources available for scheduling")
	}

	if len(nodes) == 0 {
		return nil, errors.New("no nodes available for scheduling")
	}

	err = nodeDb.Upsert(nodes)
	if err != nil {
		return nil, err
	}

	return &LegacyScheduler{
		SchedulingConfig:      schedulingConfig,
		ExecutorId:            executorId,
		TotalResources:        totalResources,
		NodeDb:                nodeDb,
		JobRepository:         jobRepository,
		PriorityFactorByQueue: priorityFactorByQueue,
		Rand:                  rand.New(rand.NewSource(rand.Int63())),
	}, nil
}

// Schedule is similar to distributeRemainder, but is built on NodeDb.
func (c *LegacyScheduler) Schedule(
	ctx context.Context,
	initialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) ([]*api.Job, map[string]*JobSchedulingReport, error) {
	log := ctxlogrus.Extract(ctx)

	// Total resource usage across all priorities by queue.
	totalResourcesByQueue := make(map[string]schedulerobjects.ResourceList)
	for queue, quantityByPriorityAndResourceType := range initialUsageByQueue {
		totalResourcesByQueue[queue] = quantityByPriorityAndResourceType.AggregateByResource()
	}

	// Iterator over (potentially) schedulable jobs for each queue.
	iteratorsByQueue := make(map[string]*QueueCandidateJobsIterator)
	for queue := range c.PriorityFactorByQueue {
		it, err := NewQueueCandidateJobsIterator(ctx, queue, initialUsageByQueue[queue].AggregateByResource(), initialUsageByQueue[queue], *c)
		if err != nil {
			return nil, nil, err
		}
		it.schedulingReportsRepository = c.SchedulingReportsRepository
		iteratorsByQueue[queue] = it
	}

	// Total resources assigned during this invocation of the scheduler.
	roundResources := schedulerobjects.ResourceList{
		Resources: make(map[string]resource.Quantity),
	}

	mostRecentSuccessfulJobSchedulingReportByQueue := make(map[string]*JobSchedulingReport)

	// Schedule jobs one at a time.
	numJobsToLease := 0
	jobsToLeaseByQueue := make(map[string][]*api.Job)
	for (c.SchedulingConfig.MaximumJobsToSchedule == 0 || numJobsToLease < c.SchedulingConfig.MaximumJobsToSchedule) && len(c.PriorityFactorByQueue) > 0 {

		// Select a queue to schedule job from.
		// Queues with fewer resources allocated to them are selected with higher probability.
		weights := WeightsFromAggregatedUsageByQueue(
			c.SchedulingConfig.ResourceScarcity, // TODO: May want to use util.GetResourceScarcity for pool-specific values.
			c.PriorityFactorByQueue,
			totalResourcesByQueue,
		)
		queue, _ := pickQueueRandomly(weights, c.Rand)

		// Schedule one job from this queue.
		it, ok := iteratorsByQueue[queue]
		if !ok {
			log.Errorf("iterator missing for queue %s", queue)
			delete(c.PriorityFactorByQueue, queue)
			continue
		}
		for {
			report, err := it.Next()
			if err != nil {
				return nil, nil, err
			}
			if report == nil { // We've processed all jobs for this queue.
				delete(c.PriorityFactorByQueue, queue)
				break
			}

			// Check overall per-round resource limits.
			// Add the resource requests of this job to the total usage for this queue.
			//
			// TODO: Account for resource usage separately by priority.
			jobTotalResourceRequests := common.TotalJobResourceRequest(report.Job)
			roundResourcesCopy := roundResources.DeepCopy()
			for resourceType, quantity := range jobTotalResourceRequests {
				q := roundResourcesCopy.Resources[resourceType]
				q.Add(quantity)
				roundResourcesCopy.Resources[resourceType] = q
			}
			if exceeded, reason := c.exceedsResourceLimits(
				ctx,
				roundResourcesCopy,
				c.SchedulingConfig.MaximalClusterFractionToSchedule,
			); exceeded {
				report.UnschedulableReason = reason + " (overall per scheduling round limit)"
				if c.SchedulingReportsRepository != nil {
					c.SchedulingReportsRepository.Add(queue, report)
				}
				continue
			}

			// Find a node for this job.
			// We need to do this here in addition to above,
			// since it may no longer be possible to assign this job to the node in the report
			// if other jobs were scheduled onto that node in the interim.
			//
			// TODO: Only repeat this process if the node in the report no longer works.
			podReport, err := c.selectNodeForPod(ctx, report.JobId, report.Job)
			if err != nil {
				return nil, nil, err
			}
			report.PodSchedulingReports = []*PodSchedulingReport{podReport}
			if podReport.Node == nil {
				report.UnschedulableReason = "pod does not fit on any node"
				if c.SchedulingReportsRepository != nil {
					c.SchedulingReportsRepository.Add(queue, report)
				}
				continue // Found no node for this job.
			}

			// Mark the job to be leased, and update resource accounting.
			it.Lease(report)
			jobsToLeaseByQueue[queue] = append(jobsToLeaseByQueue[queue], report.Job)
			numJobsToLease++
			roundResources = roundResourcesCopy
			totalResourcesByQueue[queue] = report.TotalQueueResources
			mostRecentSuccessfulJobSchedulingReportByQueue[queue] = report
			if c.SchedulingReportsRepository != nil {
				// Zero out the job spec to reduce memory usage.
				report.Job = nil
				c.SchedulingReportsRepository.Add(queue, report)
			}
			break
		}
	}

	// Try to create leases.
	jobs := make([]*api.Job, 0, numJobsToLease)
	for queue, jobsToLease := range jobsToLeaseByQueue {

		// TryLeaseJobs returns a list of jobs that were successfully leased.
		// For example, jobs concurrently leased to another executor are skipped.
		//
		// TODO: Reports generated above will be incorrect if creating the lease fails.
		successfullyLeasedJobs, err := c.JobRepository.TryLeaseJobs(c.ExecutorId, queue, jobsToLease)
		if err != nil {
			logging.WithStacktrace(log, err).Error("failed to lease jobs")
		}
		jobs = append(jobs, successfullyLeasedJobs...)
	}
	return jobs, mostRecentSuccessfulJobSchedulingReportByQueue, nil
}

func WeightsFromAggregatedUsageByQueue(resourceScarcity map[string]float64, priorityFactorByQueue map[string]float64, aggregateResourceUsageByQueue map[string]schedulerobjects.ResourceList) map[string]float64 {
	rv := make(map[string]float64)
	for queue, priorityFactor := range priorityFactorByQueue {
		if rl, ok := aggregateResourceUsageByQueue[queue]; ok {
			rv[queue] = priorityFactor / (ResourceListAsWeightedApproximateFloat64(resourceScarcity, rl) + 1)
		} else {
			rv[queue] = priorityFactor
		}
	}
	return rv
}

func ResourceListAsWeightedApproximateFloat64(resourceScarcity map[string]float64, rl schedulerobjects.ResourceList) float64 {
	usage := 0.0
	for resourceName, quantity := range rl.Resources {
		scarcity := resourceScarcity[resourceName] // TODO: Defaults to 0.
		// TODO: Why do we have our own Float64 conversion instead of quantity.AsApproximateFloat64?
		usage += common.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

// pickQueueRandomly returns a queue randomly selected from the provided map.
// The probability of returning a particular queue AQueue is shares[AQueue] / sharesSum,
// where sharesSum is the sum of all values in the provided map.
func pickQueueRandomly(shares map[string]float64, random *rand.Rand) (string, float64) {
	if len(shares) == 0 {
		return "", 0
	}

	// Generate a random number between 0 and sum.
	sum := 0.0
	for _, share := range shares {
		sum += share
	}
	pick := sum * random.Float64()
	current := 0.0

	// Iterate over queues in deterministic order.
	queues := maps.Keys(shares)
	slices.Sort(queues)

	// Select the queue as indicated by pick.
	for _, queue := range queues {
		share := shares[queue]
		current += share
		if current >= pick {
			return queue, share / sum
		}

	}
	log.Error("Could not randomly pick a queue, this should not happen!")
	queue := queues[len(queues)-1]
	return queue, shares[queue] / sum
}

func podSpecFromJob(job *api.Job) *v1.PodSpec {
	if job.PodSpec != nil {
		return job.PodSpec
	}
	for _, podSpec := range job.PodSpecs {
		if podSpec != nil {
			return podSpec
		}
	}
	return nil
}
