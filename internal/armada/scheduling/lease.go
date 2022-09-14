package scheduling

import (
	"context"
	"math"
	"math/rand"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

const leaseDeadlineTolerance = time.Second * 3

type JobQueue interface {
	PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

type leaseContext struct {
	schedulingConfig *configuration.SchedulingConfig
	queue            JobQueue
	onJobsLeased     func([]*api.Job)

	clusterId string

	queueSchedulingInfo map[*api.Queue]*QueueSchedulingInfo
	resourceScarcity    map[string]float64
	priorities          map[*api.Queue]QueuePriorityInfo

	nodeResources  []*nodeTypeAllocation
	minimumJobSize map[string]resource.Quantity

	queueCache map[string][]*api.Job
}

// LeaseJobs is the point of entry for requesting jobs to be leased to an executor.
// I.e., this function is called from the lease request gRPC endpoint.
// This function creates a leaseContext (a struct composed of all info necessary for scheduling)
// and calls leaseContext.scheduleJobs, which returns a list of leased jobs.
func LeaseJobs(ctx context.Context,
	config *configuration.SchedulingConfig, // Scheduler settings.
	jobQueue JobQueue, // Job repository.
	onJobLease func([]*api.Job), // Function to be called for all leased jobs.
	request *api.LeaseRequest, // The gRPC lease request message.
	nodeResources []*nodeTypeAllocation, // Resources available per node type.
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue,
) ([]*api.Job, error) {
	lc := newLeaseContext(
		config,
		jobQueue,
		onJobLease,
		request,
		nodeResources,
		activeClusterReports,
		activeClusterLeaseJobReports,
		clusterPriorities,
		activeQueues,
	)

	schedulingLimit := newLeasePayloadLimit(config.MaximumJobsToSchedule, config.MaximumLeasePayloadSizeBytes, int(config.MaxPodSpecSizeBytes))

	jobs, err := lc.scheduleJobs(ctx, schedulingLimit)
	if err != nil {
		return nil, errors.Errorf("[LeaseJobs] error scheduling jobs: %s", err)
	}

	return jobs, nil
}

func newLeaseContext(
	config *configuration.SchedulingConfig,
	jobQueue JobQueue,
	onJobLease func([]*api.Job),
	request *api.LeaseRequest,
	nodeResources []*nodeTypeAllocation,
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue,
) *leaseContext {
	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]

	totalCapacity := &common.ComputeResources{}
	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(util.GetClusterAvailableCapacity(clusterReport))
	}

	resourceAllocatedByQueue := CombineLeasedReportResourceByQueue(activeClusterLeaseJobReports)
	maxResourceToSchedulePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionToSchedulePerQueue)
	maxResourcePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionPerQueue)
	queueSchedulingInfo := calculateQueueSchedulingLimits(
		activeQueues,
		maxResourceToSchedulePerQueue,
		maxResourcePerQueue,
		totalCapacity,
		resourceAllocatedByQueue,
	)

	if ok {
		capacity := util.GetClusterCapacity(currentClusterReport)
		resourcesToSchedule = resourcesToSchedule.LimitWith(capacity.MulByResource(config.MaximalClusterFractionToSchedule))
	}

	activeQueuePriority := CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, activeQueues)
	scarcity := config.GetResourceScarcity(request.Pool)
	if scarcity == nil {
		scarcity = ResourceScarcityFromReports(activeClusterReports)
	}
	activeQueueSchedulingInfo := SliceResourceWithLimits(scarcity, queueSchedulingInfo, activeQueuePriority, resourcesToSchedule)

	return &leaseContext{
		schedulingConfig: config,
		queue:            jobQueue,

		clusterId: request.ClusterId,

		resourceScarcity:    scarcity,
		queueSchedulingInfo: activeQueueSchedulingInfo,
		priorities:          activeQueuePriority,
		nodeResources:       nodeResources,
		minimumJobSize:      request.MinimumJobSize,

		queueCache: map[string][]*api.Job{},

		onJobsLeased: onJobLease,
	}
}

func calculateQueueSchedulingLimits(
	activeQueues []*api.Queue,
	schedulingLimitPerQueue common.ComputeResourcesFloat,
	resourceLimitPerQueue common.ComputeResourcesFloat,
	totalCapacity *common.ComputeResources,
	currentQueueResourceAllocation map[string]common.ComputeResources,
) map[*api.Queue]*QueueSchedulingInfo {
	schedulingInfo := make(map[*api.Queue]*QueueSchedulingInfo, len(activeQueues))
	for _, queue := range activeQueues {
		remainingGlobalLimit := resourceLimitPerQueue.DeepCopy()
		if len(queue.ResourceLimits) > 0 {
			customQueueLimit := totalCapacity.MulByResource(queue.ResourceLimits)
			remainingGlobalLimit = remainingGlobalLimit.MergeWith(customQueueLimit)
		}
		if usage, ok := currentQueueResourceAllocation[queue.Name]; ok {
			remainingGlobalLimit.Sub(usage.AsFloat())
			remainingGlobalLimit.LimitToZero()
		}

		schedulingRoundLimit := schedulingLimitPerQueue.DeepCopy()

		schedulingRoundLimit = schedulingRoundLimit.LimitWith(remainingGlobalLimit)
		schedulingInfo[queue] = NewQueueSchedulingInfo(schedulingRoundLimit, common.ComputeResourcesFloat{}, common.ComputeResourcesFloat{})
	}
	return schedulingInfo
}

// TODO Remove logging code here. Instead, log at the gRPC handlers/interceptors with more info.
func (c *leaseContext) scheduleJobs(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	var jobs []*api.Job

	if !c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		assignedJobs, err := c.assignJobs(ctx, limit)
		if err != nil {
			err = errors.Errorf("[leaseContext.scheduleJobs] error leasing jobs to cluster %s: %s", c.clusterId, err)
			log.Error(err)
			return nil, err
		}
		jobs = assignedJobs
		limit.RemoveFromRemainingLimit(jobs...)
	}

	additionalJobs, err := c.distributeRemainder(ctx, limit)
	if err != nil {
		err = errors.Errorf("[leaseContext.scheduleJobs] error leasing additional jobs to cluster %s: %s", c.clusterId, err)
		log.Error(err)
		return nil, err
	}
	jobs = append(jobs, additionalJobs...)

	if c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (using probabilistic scheduling)", len(jobs))
	} else {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (by remainder distribution: %d)", len(jobs), len(additionalJobs))
	}

	return jobs, nil
}

func (c *leaseContext) assignJobs(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, info := range c.queueSchedulingInfo {
		// TODO: partition limit by priority instead
		partitionLimit := newLeasePayloadLimit(
			limit.remainingJobCount/len(c.queueSchedulingInfo),
			limit.remainingPayloadSizeLimitBytes/len(c.queueSchedulingInfo),
			limit.maxExpectedJobSizeBytes)
		leased, remainder, e := c.leaseJobs(ctx, queue, info.adjustedShare, partitionLimit)
		if e != nil {
			log.Error(e)
			continue
		}
		scheduled := info.adjustedShare.DeepCopy()
		scheduled.Sub(remainder)
		c.queueSchedulingInfo[queue].UpdateLimits(scheduled)
		jobs = append(jobs, leased...)

		if util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}
	return jobs, nil
}

func (c *leaseContext) distributeRemainder(ctx context.Context, limit LeasePayloadLimit) ([]*api.Job, error) {
	var jobs []*api.Job
	if limit.AtLimit() {
		return jobs, nil
	}

	remainder := SumRemainingResource(c.queueSchedulingInfo)
	shares := QueueSlicesToShares(c.resourceScarcity, c.queueSchedulingInfo)

	queueCount := len(c.queueSchedulingInfo)
	emptySteps := 0

	minimumJobSize := common.ComputeResources(c.minimumJobSize).AsFloat()
	minimumResource := c.schedulingConfig.MinimumResourceToSchedule.DeepCopy()
	minimumResource.Max(minimumJobSize)

	hasEnoughResources := !remainder.IsLessThan(minimumResource)
	for hasEnoughResources && len(shares) > 0 && emptySteps < queueCount {
		queue := pickQueueRandomly(shares)
		emptySteps++

		amountToSchedule := remainder.DeepCopy()
		amountToSchedule = amountToSchedule.LimitWith(c.queueSchedulingInfo[queue].remainingSchedulingLimit)
		leaseLimit := newLeasePayloadLimit(1, limit.remainingPayloadSizeLimitBytes, limit.maxExpectedJobSizeBytes)
		leased, remaining, e := c.leaseJobs(ctx, queue, amountToSchedule, leaseLimit)
		if e != nil {
			log.Error(e)
			continue
		}
		if len(leased) > 0 {
			emptySteps = 0
			jobs = append(jobs, leased...)

			scheduled := amountToSchedule.DeepCopy()
			scheduled.Sub(remaining)

			c.queueSchedulingInfo[queue].UpdateLimits(scheduled)
			remainder.Sub(scheduled)
			shares[queue] = math.Max(0, ResourcesFloatAsUsage(c.resourceScarcity, c.queueSchedulingInfo[queue].schedulingShare))
		} else {
			// if there are no suitable jobs to lease eliminate queue from the scheduling
			delete(c.queueSchedulingInfo, queue)
			delete(c.priorities, queue)
			c.queueSchedulingInfo = SliceResourceWithLimits(c.resourceScarcity, c.queueSchedulingInfo, c.priorities, remainder)
			shares = QueueSlicesToShares(c.resourceScarcity, c.queueSchedulingInfo)
		}

		limit.RemoveFromRemainingLimit(leased...)
		if limit.AtLimit() || util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}

	return jobs, nil
}

// leaseJobs calls into the JobRepository underlying the queue contained in the leaseContext to lease jobs.
// Returns a slice of jobs that were leased.
func (c *leaseContext) leaseJobs(
	ctx context.Context,
	queue *api.Queue,
	slice common.ComputeResourcesFloat,
	limit LeasePayloadLimit,
) ([]*api.Job, common.ComputeResourcesFloat, error) {
	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {
		if limit.AtLimit() {
			break
		}

		topJobs, ok := c.queueCache[queue.Name]
		if !ok || len(topJobs) < int(c.schedulingConfig.QueueLeaseBatchSize/2) {
			newTop, e := c.queue.PeekClusterQueue(c.clusterId, queue.Name, int64(c.schedulingConfig.QueueLeaseBatchSize))
			if e != nil {
				return nil, slice, e
			}
			c.queueCache[queue.Name] = newTop
			topJobs = c.queueCache[queue.Name]
		}

		candidates := make([]*api.Job, 0)
		candidatesLimit := newLeasePayloadLimit(limit.remainingJobCount, limit.remainingPayloadSizeLimitBytes, limit.maxExpectedJobSizeBytes)
		candidateNodes := map[*api.Job]nodeTypeUsedResources{}
		consumedNodeResources := nodeTypeUsedResources{}

		for _, job := range topJobs {
			requirement := common.TotalJobResourceRequest(job).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)

			if isJobSchedulable(c, job, remainder, candidatesLimit) {
				if hasPriorityClass(job.PodSpec) {
					validateOrDefaultPriorityClass(job.PodSpec, c.schedulingConfig.Preemption)
				}
				newlyConsumed, ok, _ := matchAnyNodeTypeAllocation(
					job,
					c.nodeResources,
					consumedNodeResources,
					c.schedulingConfig.Preemption.PriorityClasses,
				)
				if ok {
					slice = remainder
					candidates = append(candidates, job)
					candidatesLimit.RemoveFromRemainingLimit(job)
					candidateNodes[job] = newlyConsumed
					consumedNodeResources.Add(newlyConsumed)
				}
			}
			if candidatesLimit.AtLimit() {
				break
			}
		}
		c.queueCache[queue.Name] = removeJobs(c.queueCache[queue.Name], candidates)

		leased, e := c.queue.TryLeaseJobs(c.clusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)
		limit.RemoveFromRemainingLimit(leased...)

		c.decreaseNodeResources(leased, candidateNodes)

		// stop scheduling round if we leased less then batch (either the slice is too small or queue is empty)
		// TODO: should we look at next batch?
		if len(candidates) < int(c.schedulingConfig.QueueLeaseBatchSize) {
			break
		}
		if util.CloseToDeadline(ctx, leaseDeadlineTolerance) {
			break
		}
	}

	go c.onJobsLeased(jobs)

	return jobs, slice, nil
}

func isJobSchedulable(c *leaseContext, job *api.Job, remainder common.ComputeResourcesFloat, candidatesLimit LeasePayloadLimit) bool {
	isJobLargeEnough := isLargeEnough(job, c.minimumJobSize)
	isRemainderValid := remainder.IsValid()
	isCandidateWithinLimit := candidatesLimit.IsWithinLimit(job)

	isRegularlySchedulable := isJobLargeEnough && isRemainderValid && isCandidateWithinLimit
	isPreemptiveJob := isJobLargeEnough && hasPriorityClass(job.PodSpec)

	return isRegularlySchedulable || isPreemptiveJob
}

// validateOrDefaultPriorityClass checks is the pod spec's priority class configured as supported in Server config
// if not, default to DefaultPriorityClass if it is specified
// otherwise default to no Priority Class
func validateOrDefaultPriorityClass(podSpec *v1.PodSpec, preemptionConfig configuration.PreemptionConfig) {
	if preemptionConfig.PriorityClasses == nil {
		podSpec.PriorityClassName = preemptionConfig.DefaultPriorityClass
	}
	_, ok := preemptionConfig.PriorityClasses[podSpec.PriorityClassName]
	if !ok {
		podSpec.PriorityClassName = preemptionConfig.DefaultPriorityClass
	}
}

func (c *leaseContext) decreaseNodeResources(leased []*api.Job, nodeTypeUsage map[*api.Job]nodeTypeUsedResources) {
	for _, j := range leased {
		for nodeType, resources := range nodeTypeUsage[j] {
			nodeType.availableResources.Sub(resources)
		}
	}
}

// removeJobs returns the subset of jobs not in jobsToRemove.
func removeJobs(jobs []*api.Job, jobsToRemove []*api.Job) []*api.Job {
	jobsToRemoveIds := make(map[string]bool, len(jobsToRemove))
	for _, job := range jobsToRemove {
		jobsToRemoveIds[job.Id] = true
	}

	result := make([]*api.Job, 0, len(jobs))
	for _, job := range jobs {
		if _, shouldRemove := jobsToRemoveIds[job.Id]; !shouldRemove {
			result = append(result, job)
		}
	}
	return result
}

// pickQueueRandomly returns a queue randomly selected from the provided map.
// The probability of returning a particular queue AQueue is shares[AQueue] / sharesSum,
// where sharesSum is the sum of all values in the provided map.
func pickQueueRandomly(shares map[*api.Queue]float64) *api.Queue {
	sum := 0.0
	for _, share := range shares {
		sum += share
	}

	pick := sum * rand.Float64()
	current := 0.0

	var lastQueue *api.Queue
	for queue, share := range shares {
		current += share
		if current >= pick {
			return queue
		}
		lastQueue = queue
	}
	log.Error("Could not randomly pick a queue, this should not happen!")
	return lastQueue
}
