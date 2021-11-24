package scheduling

import (
	"context"
	"math"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

type JobQueue interface {
	PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
}

type leaseContext struct {
	schedulingConfig *configuration.SchedulingConfig
	queue            JobQueue
	onJobsLeased     func([]*api.Job)

	ctx       context.Context
	clusterId string

	queueSchedulingInfo map[*api.Queue]*QueueSchedulingInfo
	resourceScarcity    map[string]float64
	priorities          map[*api.Queue]QueuePriorityInfo

	nodeResources  []*nodeTypeAllocation
	minimumJobSize map[string]resource.Quantity

	queueCache map[string][]*api.Job
}

type SchedulingLimit struct {
	numberJobsLeftToSchedule       int
	remainingPayloadSizeToSchedule int
	minimumRemaningPayloadSize     int
}

func NewSchedulingLimit(numberOfJobsLimit int, payloadSizeLimitBytes int, minimumRemaningPayloadSize int) SchedulingLimit {
	return SchedulingLimit{
		numberJobsLeftToSchedule:       numberOfJobsLimit,
		remainingPayloadSizeToSchedule: payloadSizeLimitBytes,
		minimumRemaningPayloadSize:     minimumRemaningPayloadSize,
	}
}

func (s *SchedulingLimit) RemoveFromRemainingLimit(jobs ...*api.Job) {
	for _, job := range jobs {
		s.numberJobsLeftToSchedule -= 1
		s.remainingPayloadSizeToSchedule -= job.Size()
	}
}

func (s *SchedulingLimit) AtLimit() bool {
	return s.numberJobsLeftToSchedule <= 0 || s.remainingPayloadSizeToSchedule <= s.minimumRemaningPayloadSize
}

func (s *SchedulingLimit) IsWithinLimit(job *api.Job) bool {
	return s.numberJobsLeftToSchedule >= 1 && s.remainingPayloadSizeToSchedule-job.Size() >= 0
}

func LeaseJobs(ctx context.Context,
	config *configuration.SchedulingConfig,
	jobQueue JobQueue,
	onJobLease func([]*api.Job),
	request *api.LeaseRequest,
	nodeResources []*nodeTypeAllocation,
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue) ([]*api.Job, error) {

	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]

	totalCapacity := &common.ComputeResources{}
	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(util.GetClusterAvailableCapacity(clusterReport))
	}

	resourceAllocatedByQueue := CombineLeasedReportResourceByQueue(activeClusterLeaseJobReports)
	maxResourceToSchedulePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionToSchedulePerQueue)
	maxResourcePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionPerQueue)
	queueSchedulingInfo := calculateQueueSchedulingLimits(activeQueues, maxResourceToSchedulePerQueue, maxResourcePerQueue, totalCapacity, resourceAllocatedByQueue)

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

	lc := &leaseContext{
		schedulingConfig: config,
		queue:            jobQueue,

		ctx:       ctx,
		clusterId: request.ClusterId,

		resourceScarcity:    scarcity,
		queueSchedulingInfo: activeQueueSchedulingInfo,
		priorities:          activeQueuePriority,
		nodeResources:       nodeResources,
		minimumJobSize:      request.MinimumJobSize,

		queueCache: map[string][]*api.Job{},

		onJobsLeased: onJobLease,
	}

	schedulingLimit := NewSchedulingLimit(config.MaximumJobsToSchedule, config.MaximumLeasePayloadSizeBytes, int(config.MaxPodSpecSizeBytes)*2)
	return lc.scheduleJobs(schedulingLimit)
}

func calculateQueueSchedulingLimits(
	activeQueues []*api.Queue,
	schedulingLimitPerQueue common.ComputeResourcesFloat,
	resourceLimitPerQueue common.ComputeResourcesFloat,
	totalCapacity *common.ComputeResources,
	currentQueueResourceAllocation map[string]common.ComputeResources) map[*api.Queue]*QueueSchedulingInfo {
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

func (c *leaseContext) scheduleJobs(limit SchedulingLimit) ([]*api.Job, error) {
	jobs := []*api.Job{}

	if !c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		assignedJobs, e := c.assignJobs(limit)
		if e != nil {
			log.Errorf("Error when leasing jobs for cluster %s: %s", c.clusterId, e)
			return nil, e
		}
		jobs = assignedJobs
		limit.RemoveFromRemainingLimit(jobs...)
	}

	additionalJobs, e := c.distributeRemainder(limit)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", c.clusterId, e)
		return nil, e
	}
	jobs = append(jobs, additionalJobs...)

	if c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (using probabilistic scheduling)", len(jobs))
	} else {
		log.WithField("clusterId", c.clusterId).Infof("Leasing %d jobs. (by remainder distribution: %d)", len(jobs), len(additionalJobs))
	}

	return jobs, nil
}

func (c *leaseContext) assignJobs(limit SchedulingLimit) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, info := range c.queueSchedulingInfo {
		// TODO: partition limit by priority instead
		partitionLimit := NewSchedulingLimit(
			limit.numberJobsLeftToSchedule/len(c.queueSchedulingInfo),
			limit.remainingPayloadSizeToSchedule/limit.numberJobsLeftToSchedule/len(c.queueSchedulingInfo),
			limit.minimumRemaningPayloadSize)
		leased, remainder, e := c.leaseJobs(queue, info.adjustedShare, partitionLimit)
		if e != nil {
			log.Error(e)
			continue
		}
		scheduled := info.adjustedShare.DeepCopy()
		scheduled.Sub(remainder)
		c.queueSchedulingInfo[queue].UpdateLimits(scheduled)
		jobs = append(jobs, leased...)

		if c.closeToDeadline() {
			break
		}
	}
	return jobs, nil
}

func (c *leaseContext) distributeRemainder(limit SchedulingLimit) ([]*api.Job, error) {

	jobs := []*api.Job{}
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

	for !remainder.IsLessThan(minimumResource) && len(shares) > 0 && emptySteps < queueCount {
		queue := pickQueueRandomly(shares)
		emptySteps++

		amountToSchedule := remainder.DeepCopy()
		amountToSchedule = amountToSchedule.LimitWith(c.queueSchedulingInfo[queue].remainingSchedulingLimit)
		leaseLimit := NewSchedulingLimit(1, limit.remainingPayloadSizeToSchedule, limit.minimumRemaningPayloadSize)
		leased, remaining, e := c.leaseJobs(queue, amountToSchedule, leaseLimit)
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
		if limit.AtLimit() || c.closeToDeadline() {
			break
		}
	}

	return jobs, nil
}

func (c *leaseContext) leaseJobs(queue *api.Queue, slice common.ComputeResourcesFloat, limit SchedulingLimit) ([]*api.Job, common.ComputeResourcesFloat, error) {
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
		candidatesLimit := NewSchedulingLimit(limit.numberJobsLeftToSchedule, limit.remainingPayloadSizeToSchedule, limit.minimumRemaningPayloadSize)
		candidateNodes := map[*api.Job]nodeTypeUsedResources{}
		consumedNodeResources := nodeTypeUsedResources{}

		for _, job := range topJobs {
			requirement := common.TotalJobResourceRequest(job).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if isLargeEnough(job, c.minimumJobSize) && remainder.IsValid() && candidatesLimit.IsWithinLimit(job) {
				newlyConsumed, ok := matchAnyNodeTypeAllocation(job, c.nodeResources, consumedNodeResources)
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
		if c.closeToDeadline() {
			break
		}
	}

	go c.onJobsLeased(jobs)

	return jobs, slice, nil
}

func (c *leaseContext) decreaseNodeResources(leased []*api.Job, nodeTypeUsage map[*api.Job]nodeTypeUsedResources) {
	for _, j := range leased {
		for nodeType, resources := range nodeTypeUsage[j] {
			nodeType.availableResources.Sub(resources)
		}
	}
}

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

func (c *leaseContext) closeToDeadline() bool {
	d, exists := c.ctx.Deadline()
	return exists && d.Before(time.Now().Add(time.Second))
}

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
