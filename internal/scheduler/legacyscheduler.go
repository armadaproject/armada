package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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

type LegacyScheduler struct {
	SchedulingConfig configuration.SchedulingConfig
	// Executor for which we're currently scheduling jobs.
	ExecutorId string
	// Used for matching pods with nodes.
	NodeDb *NodeDb
	// Used to request jobs from Redis and to mark jobs as leased.
	JobQueue scheduling.JobQueue
	// Minimum quantity allowed for jobs leased to this cluster.
	MinimumJobSize map[string]resource.Quantity
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactorByQueue map[string]float64
	// Base random seed used for the the scheduling loop.
	// The first iteration uses InitialSeed + 1, the second InitialSeed + 2, and so on.
	InitialSeed int64
	// Store reports for each scheduling attempt.
	JobSchedulingReportsByQueue map[string]map[uuid.UUID]*JobSchedulingReport
}

// Schedule is similar to distributeRemainder, but is built on NodeDb.
func (c *LegacyScheduler) Schedule(
	ctx context.Context,
	initialUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) ([]*api.Job, error) {
	log := ctxlogrus.Extract(ctx)

	// Total resource usage across all priorities by queue.
	totalResourcesByQueue := make(map[string]schedulerobjects.ResourceList)
	for queue, quantityByPriorityAndResourceType := range initialUsageByQueue {
		totalResourcesByQueue[queue] = quantityByPriorityAndResourceType.AggregateByResource()
	}

	// Initialise reports dict if not done already.
	if c.JobSchedulingReportsByQueue == nil {
		c.JobSchedulingReportsByQueue = make(map[string]map[uuid.UUID]*JobSchedulingReport)
	}

	// Total resources assigned during this invocation of the scheduler.
	roundResources := schedulerobjects.ResourceList{
		Resources: make(map[string]resource.Quantity),
	}

	// Total resources assigned to each queue during this invocation of the scheduler.
	roundResourcesByQueue := make(map[string]schedulerobjects.ResourceList)

	// Jobs to lease for each queue.
	leasedJobsByQueue := make(map[string][]*api.Job)

	// Track the total number of jobs to lease.
	numJobsToLease := 0

	// To reduce the number of calls to Redis,
	// we retrieve jobs in batch and store jobs in-memory.
	jobCacheByQueue := make(map[string][]*api.Job)

	// Used to return early if the scheduler makes no progress.
	consecutiveIterationsWithNoJobsLeased := 0

	// Maps queue name to a bool, which is true if there are no more
	// jobs in this queue, beyond those already downloaded.
	gotAllQueuedJobsByQueue := make(map[string]bool)

	// Random seed used when selecting queues.
	// Provided as a paremeter to ensure the scheduler is deterministic when testing.
	seed := c.InitialSeed

	// Schedule jobs one at a time.
	for (c.SchedulingConfig.MaximumJobsToSchedule == 0 || numJobsToLease < c.SchedulingConfig.MaximumJobsToSchedule) && len(totalResourcesByQueue) > 0 && consecutiveIterationsWithNoJobsLeased < len(totalResourcesByQueue) {

		// Return early if the context deadline has expired.
		select {
		case <-ctx.Done():
			break
		default:
		}

		fmt.Println()
		fmt.Println("=======")

		fmt.Println("iteration ", seed)

		// Select a queue to schedule job from.
		// Queues with fewer resources allocated to them are selected with higher propability.
		seed += 1
		shares := WeightsFromAggregatedUsageByQueue(
			c.SchedulingConfig.ResourceScarcity,
			c.PriorityFactorByQueue,
			totalResourcesByQueue,
		)
		queue, _ := pickQueueRandomly(shares, seed)
		consecutiveIterationsWithNoJobsLeased++

		fmt.Println("queue ", queue, " shares: ", shares)

		// Total resource usage (across priorities) for this queue.
		totalResourcesForQueue, ok := totalResourcesByQueue[queue]
		if !ok {
			totalResourcesForQueue = schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
			totalResourcesByQueue[queue] = totalResourcesForQueue
		}
		totalResourcesForQueue = totalResourcesForQueue.DeepCopy()

		// Total resources (across priorities) assigned for this queue during this invocation of the scheduler.
		roundResourcesForQueue, ok := roundResourcesByQueue[queue]
		if !ok {
			roundResourcesForQueue = schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
			roundResourcesByQueue[queue] = roundResourcesForQueue
		}
		roundResourcesForQueue = roundResourcesForQueue.DeepCopy()

		// Total resources (across priorities) assigned during this invocation of the scheduler.
		roundResourcesCopy := roundResources.DeepCopy()

		// To avoid querying the database for jobs at each iteration,
		// retrieve jobs only the first time we select a queue.
		candidateJobs, ok := jobCacheByQueue[queue]
		if !ok {
			var err error
			batchSize := int64(c.SchedulingConfig.QueueLeaseBatchSize)
			if batchSize == 0 {
				// Use a default batch size of 100 if not set.
				batchSize = 100
			}
			candidateJobs, err = c.JobQueue.PeekClusterQueue(
				c.ExecutorId,
				queue,
				batchSize,
			)
			if err != nil {
				return nil, err
			}
			jobCacheByQueue[queue] = candidateJobs
			gotAllQueuedJobsByQueue[queue] = len(candidateJobs) == int(batchSize)
		}
		fmt.Println(len(candidateJobs), " candidate jobs")
		if len(candidateJobs) == 0 {
			continue
		}

		// Pop one job from the candidate list.
		candidateJob := candidateJobs[0]
		jobCacheByQueue[queue] = candidateJobs[1:]

		fmt.Println("selected ", candidateJob.Id, " ", len(jobCacheByQueue[queue]), " remaining")

		// Convert the string representation of a job id to a uuid.UUID.
		jobIdProto, err := armadaevents.ProtoUuidFromUlidString(candidateJob.Id)
		if err != nil {
			logging.WithStacktrace(log, err).Errorf("failed to parse %s into uuid", candidateJob.Id)
			continue
		}
		jobId := armadaevents.UuidFromProtoUuid(jobIdProto)

		// Create a scheduling report for this job.
		jobSchedulingReport := &JobSchedulingReport{
			Timestamp: time.Now(),
			JobId:     jobId,
		}
		if m, ok := c.JobSchedulingReportsByQueue[queue]; ok {
			m[jobId] = jobSchedulingReport
		} else {
			c.JobSchedulingReportsByQueue[queue] = map[uuid.UUID]*JobSchedulingReport{
				jobId: jobSchedulingReport,
			}
		}

		// Add the resource requests of this job to the total usage for this queue.
		//
		// TODO: Account for resource usage separately by priority.
		jobTotalResourceRequests := common.TotalJobResourceRequest(candidateJob)
		for resourceType, quantity := range jobTotalResourceRequests {
			q := totalResourcesForQueue.Resources[resourceType]
			q.Add(quantity)
			totalResourcesForQueue.Resources[resourceType] = q

			q = roundResourcesForQueue.Resources[resourceType]
			q.Add(quantity)
			roundResourcesForQueue.Resources[resourceType] = q

			q = roundResourcesCopy.Resources[resourceType]
			q.Add(quantity)
			roundResourcesCopy.Resources[resourceType] = q
		}

		// Check that this job is at least equal to the minimum job size.
		// TODO: These per-job checks could be expressed as filter functions, e.g., of type
		// jobsFilterFunc func(*api.Job) bool
		jobTooSmall := false
		if len(c.MinimumJobSize) > 0 {
			for resourceType, quantity := range jobTotalResourceRequests {
				if limit, ok := c.MinimumJobSize[resourceType]; ok {
					if quantity.Cmp(limit) != -1 {
						jobTooSmall = true
						jobSchedulingReport.UnschedulableReason = fmt.Sprintf(
							"job requests %s %s, but the minimum is %s",
							quantity.String(), resourceType, limit.String(),
						)
						break
					}
				}
			}
		}
		if jobTooSmall {
			continue
		}

		// Check if scheduling this job would exceed per-queue resource limits.
		queueTotalResourceLimitsExceeded := false
		for resourceType, limit := range c.SchedulingConfig.MaximalClusterFractionToSchedule {
			totalAmount := c.NodeDb.totalResources[resourceType]
			amountUsedByQueue := totalResourcesForQueue.Resources[resourceType]
			if amountUsedByQueue.AsApproximateFloat64()/totalAmount.AsApproximateFloat64() > limit {
				queueTotalResourceLimitsExceeded = true
				break
			}
		}
		if queueTotalResourceLimitsExceeded {
			jobSchedulingReport.UnschedulableReason = "queueTotalResourceLimitsExceeded"
			continue
		}

		// Check if scheduling this job would exceed per-queue resource limits for this round.
		queueRoundResourceLimitsExceeded := false
		for resourceType, limit := range c.SchedulingConfig.MaximalResourceFractionToSchedulePerQueue {
			totalAmount := c.NodeDb.totalResources[resourceType]
			amountUsedByQueue := roundResourcesForQueue.Resources[resourceType]
			if amountUsedByQueue.AsApproximateFloat64()/totalAmount.AsApproximateFloat64() > limit {
				queueRoundResourceLimitsExceeded = true
				break
			}
		}
		if queueRoundResourceLimitsExceeded {
			jobSchedulingReport.UnschedulableReason = "queueRoundResourceLimitsExceeded"
			continue
		}

		// Check if scheduling this job would exceed resource limits for this round.
		roundResourceLimitsExceeded := false
		for resourceType, limit := range c.SchedulingConfig.MaximalClusterFractionToSchedule {
			totalAmount := c.NodeDb.totalResources[resourceType]
			amountUsed := roundResourcesCopy.Resources[resourceType]
			if amountUsed.AsApproximateFloat64()/totalAmount.AsApproximateFloat64() > limit {
				roundResourceLimitsExceeded = true
				break
			}
		}
		if roundResourceLimitsExceeded {
			jobSchedulingReport.UnschedulableReason = "roundResourceLimitsExceeded"
			continue
		}

		podSpec := podSpecFromJob(candidateJob)
		if podSpec == nil {
			log.Errorf("failed to get pod for job with id %s", candidateJob.Id)
			jobSchedulingReport.UnschedulableReason = "failedToGetPodSpec"
			continue
		}

		// Try to find a node for this pod.
		// Store the report returned by the NodeDb.
		req := schedulerobjects.PodRequirementsFromPodSpec(podSpec)
		report, err := c.NodeDb.SelectAndBindNodeToPod(jobId, req)
		if err != nil {
			logging.WithStacktrace(log, err).Error("error selecting node for pod")
			return nil, err
		}
		jobSchedulingReport.PodSchedulingReports = append(jobSchedulingReport.PodSchedulingReports, report)

		// Could not find a node for this pod.
		if report.Node == nil {
			jobSchedulingReport.UnschedulableReason = "failedToSchedulePod"
			continue
		}

		// The job can be scheduled.
		leasedJobsByQueue[queue] = append(leasedJobsByQueue[queue], candidateJob)

		// Update the resource accounting for this queue.
		totalResourcesByQueue[queue] = totalResourcesForQueue
		roundResourcesByQueue[queue] = roundResourcesForQueue
		roundResources = roundResourcesCopy

		fmt.Println("queue resource usage: ", totalResourcesByQueue[queue])
		consecutiveIterationsWithNoJobsLeased = 0
		numJobsToLease += 1

		// Exit if we've processed all jobs for some queue.
		// Since continuing would be unfair to that queue.
		if len(jobCacheByQueue[queue]) == 0 && gotAllQueuedJobsByQueue[queue] {
			break
		}
	}

	jobs := make([]*api.Job, 0)
	for queue, jobsToLease := range leasedJobsByQueue {

		// TryLeaseJobs returns a list of jobs that were successfully leased.
		// For example, jobs concurrently leased to another executor are skipped.
		//
		// TODO: Reports generated above will be incorrect if creating the lease fails.
		successfullyLeasedJobs, err := c.JobQueue.TryLeaseJobs(c.ExecutorId, queue, jobsToLease)
		if err != nil {
			logging.WithStacktrace(log, err).Error("failed to lease jobs")
		}
		jobs = append(jobs, successfullyLeasedJobs...)
	}

	return jobs, nil
}

func WeightsFromAggregatedUsageByQueue(resourceScarcity map[string]float64, priorityFactorByQueue map[string]float64, aggregateResourceUsageByQueue map[string]schedulerobjects.ResourceList) map[string]float64 {
	shares := make(map[string]float64)
	for queue, rl := range aggregateResourceUsageByQueue {
		priorityFactor, ok := priorityFactorByQueue[queue]
		if !ok {
			priorityFactor = 1
		}
		shares[queue] = priorityFactor / (ResourceListAsWeightedApproximateFloat64(resourceScarcity, rl) + 1)
	}
	return shares
}

func ResourceListAsWeightedApproximateFloat64(resourceScarcity map[string]float64, rl schedulerobjects.ResourceList) float64 {
	usage := 0.0
	for resourceName, quantity := range rl.Resources {
		scarcity := resourceScarcity[resourceName]
		// TODO: Why do we have our own Float64 conversion instead of quantity.AsApproximateFloat64?
		usage += common.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

// pickQueueRandomly returns a queue randomly selected from the provided map.
// The probability of returning a particular queue AQueue is shares[AQueue] / sharesSum,
// where sharesSum is the sum of all values in the provided map.
func pickQueueRandomly(shares map[string]float64, seed int64) (string, float64) {
	if len(shares) == 0 {
		return "", 0
	}

	// Generate a random number between 0 and sum.
	sum := 0.0
	for _, share := range shares {
		sum += share
	}
	pick := sum * rand.New(rand.NewSource(seed)).Float64()
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
