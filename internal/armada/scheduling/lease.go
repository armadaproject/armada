package scheduling

import (
	"context"
	"math"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

const maxJobsPerLease = 10000

type leaseContext struct {
	schedulingConfig *configuration.SchedulingConfig
	repository       repository.JobQueueRepository
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

type nodeTypeAllocation struct {
	taints             []v1.Taint
	labels             map[string]string
	nodeSize           common.ComputeResources
	availableResources common.ComputeResourcesFloat
}

func LeaseJobs(ctx context.Context,
	config *configuration.SchedulingConfig,
	jobQueueRepository repository.JobQueueRepository,
	onJobLease func([]*api.Job), request *api.LeaseRequest,
	nodeResources []*nodeTypeAllocation,
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue) ([]*api.Job, error) {

	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]

	totalCapacity := &common.ComputeResources{}
	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(clusterReport.ClusterAvailableCapacity)
	}

	resourceAllocatedByQueue := CombineLeasedReportResourceByQueue(activeClusterLeaseJobReports)
	maxResourceToSchedulePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionToSchedulePerQueue)
	maxResourcePerQueue := totalCapacity.MulByResource(config.MaximalResourceFractionPerQueue)
	queueSchedulingInfo := calculateQueueSchedulingLimits(activeQueues, maxResourceToSchedulePerQueue, maxResourcePerQueue, totalCapacity, resourceAllocatedByQueue)

	if ok {
		capacity := common.ComputeResources(currentClusterReport.ClusterCapacity)
		resourcesToSchedule = resourcesToSchedule.LimitWith(capacity.MulByResource(config.MaximalClusterFractionToSchedule))
	}

	activeQueuePriority := CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, activeQueues)
	scarcity := ResourceScarcityFromReports(activeClusterReports)
	activeQueueSchedulingInfo := SliceResourceWithLimits(scarcity, queueSchedulingInfo, activeQueuePriority, resourcesToSchedule)

	lc := &leaseContext{
		schedulingConfig: config,
		repository:       jobQueueRepository,

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

	return lc.scheduleJobs(maxJobsPerLease)
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

func (c *leaseContext) scheduleJobs(limit int) ([]*api.Job, error) {
	jobs := []*api.Job{}

	if !c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		assignedJobs, e := c.assignJobs(limit)
		if e != nil {
			log.Errorf("Error when leasing jobs for cluster %s: %s", c.clusterId, e)
			return nil, e
		}
		jobs = assignedJobs
		limit -= len(jobs)
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

func (c *leaseContext) assignJobs(limit int) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, info := range c.queueSchedulingInfo {
		// TODO: partition limit by priority instead
		leased, remainder, e := c.leaseJobs(queue, info.adjustedShare, limit/len(c.queueSchedulingInfo))
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

func (c *leaseContext) distributeRemainder(limit int) ([]*api.Job, error) {

	jobs := []*api.Job{}
	if limit <= 0 {
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
		leased, remaining, e := c.leaseJobs(queue, amountToSchedule, 1)
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

		limit -= len(leased)
		if limit <= 0 || c.closeToDeadline() {
			break
		}
	}

	return jobs, nil
}

func (c *leaseContext) leaseJobs(queue *api.Queue, slice common.ComputeResourcesFloat, limit int) ([]*api.Job, common.ComputeResourcesFloat, error) {
	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {
		if limit <= 0 {
			break
		}

		topJobs, ok := c.queueCache[queue.Name]
		if !ok || len(topJobs) < int(c.schedulingConfig.QueueLeaseBatchSize/2) {
			newTop, e := c.repository.PeekQueue(queue.Name, int64(c.schedulingConfig.QueueLeaseBatchSize))
			if e != nil {
				return nil, slice, e
			}
			c.queueCache[queue.Name] = newTop
			topJobs = c.queueCache[queue.Name]
		}

		candidates := make([]*api.Job, 0)
		candidateNodes := map[*api.Job]*nodeTypeAllocation{}
		consumedNodeResources := map[*nodeTypeAllocation]common.ComputeResourcesFloat{}

		for _, job := range topJobs {
			requirement := common.TotalResourceRequest(job.PodSpec).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if isLargeEnough(job, c.minimumJobSize) && remainder.IsValid() {
				nodeType, ok := matchAnyNodeTypeAllocation(job, c.nodeResources, consumedNodeResources)
				if ok {
					slice = remainder
					resourceRequest := requirement.DeepCopy()
					resourceRequest.Add(consumedNodeResources[nodeType])
					consumedNodeResources[nodeType] = resourceRequest

					candidates = append(candidates, job)
					candidateNodes[job] = nodeType
				}
			}
			if len(candidates) >= limit {
				break
			}
		}
		c.queueCache[queue.Name] = removeJobs(c.queueCache[queue.Name], candidates)

		leased, e := c.repository.TryLeaseJobs(c.clusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)
		limit -= len(leased)

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

func (c *leaseContext) decreaseNodeResources(leased []*api.Job, nodeAssignment map[*api.Job]*nodeTypeAllocation) {
	for _, j := range leased {
		resources := common.TotalResourceRequest(j.PodSpec).AsFloat()
		nodeType := nodeAssignment[j]
		nodeType.availableResources.Sub(resources)
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
