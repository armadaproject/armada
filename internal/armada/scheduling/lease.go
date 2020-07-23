package scheduling

import (
	"context"
	"math"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

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

	ctx     context.Context
	request *api.LeaseRequest

	clusterSchedulingInfo *api.ClusterSchedulingInfoReport
	queueSchedulingInfo   map[*api.Queue]*QueueSchedulingInfo
	resourceScarcity      map[string]float64
	priorities            map[*api.Queue]QueuePriorityInfo

	queueCache map[string][]*api.Job
}

func LeaseJobs(
	ctx context.Context,
	config *configuration.SchedulingConfig,
	jobQueueRepository repository.JobQueueRepository,
	onJobLease func([]*api.Job),
	request *api.LeaseRequest,
	activeClusterReports map[string]*api.ClusterUsageReport,
	activeClusterLeaseJobReports map[string]*api.ClusterLeasedReport,
	clusterPriorities map[string]map[string]float64,
	activeQueues []*api.Queue,
) ([]*api.Job, error) {
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

		ctx:     ctx,
		request: request,

		clusterSchedulingInfo: CreateClusterSchedulingInfoReport(request),
		resourceScarcity:      scarcity,
		queueSchedulingInfo:   activeQueueSchedulingInfo,
		priorities:            activeQueuePriority,

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
			log.Errorf("Error when leasing jobs for cluster %s: %s", c.request.ClusterId, e)
			return nil, e
		}
		jobs = assignedJobs
		limit -= len(jobs)
	}

	additionalJobs, e := c.distributeRemainder(limit)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", c.request.ClusterId, e)
		return nil, e
	}
	jobs = append(jobs, additionalJobs...)

	if c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		log.WithField("clusterId", c.request.ClusterId).Infof("Leasing %d jobs. (using probabilistic scheduling)", len(jobs))
	} else {
		log.WithField("clusterId", c.request.ClusterId).Infof("Leasing %d jobs. (by remainder distribution: %d)", len(jobs), len(additionalJobs))
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

	minimumJobSize := common.ComputeResources(c.request.MinimumJobSize).AsFloat()
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
		for _, job := range topJobs {
			requirement := common.TotalResourceRequest(job.PodSpec).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if remainder.IsValid() &&
				MatchSchedulingRequirements(job, c.clusterSchedulingInfo) &&
				matchAnyNode(job, c.request.Nodes) {

				slice = remainder
				candidates = append(candidates, job)
			}
			if len(candidates) >= limit {
				break
			}
		}
		c.queueCache[queue.Name] = removeJobs(c.queueCache[queue.Name], candidates)

		leased, e := c.repository.TryLeaseJobs(c.request.ClusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)
		limit -= len(leased)

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

func MatchSchedulingRequirements(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) bool {
	return matchNodeLabels(job, schedulingInfo) &&
		isAbleToFitOnAvailableNodes(job, schedulingInfo) &&
		isLargeEnough(job, schedulingInfo)
}

func isAbleToFitOnAvailableNodes(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) bool {
	resourceRequest := common.TotalResourceRequest(job.PodSpec).AsFloat()
	for _, node := range schedulingInfo.NodeSizes {
		var nodeSize common.ComputeResources = node.Resources
		remainder := nodeSize.AsFloat()
		remainder.Sub(resourceRequest)
		if remainder.IsValid() {
			return true
		}
	}
	return false
}

func matchNodeLabels(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) bool {
	if len(job.PodSpec.NodeSelector) == 0 {
		return true
	}

Labels:
	for _, labeling := range schedulingInfo.AvailableLabels {
		for k, v := range job.PodSpec.NodeSelector {
			if labeling.Labels[k] != v {
				continue Labels
			}
		}
		return true
	}
	return false
}

func isLargeEnough(job *api.Job, schedulingInfo *api.ClusterSchedulingInfoReport) bool {
	resourceRequest := common.TotalResourceRequest(job.PodSpec)
	minimum := common.ComputeResources(schedulingInfo.MinimumJobSize)
	resourceRequest.Sub(minimum)
	return resourceRequest.IsValid()
}

func matchAnyNode(job *api.Job, nodes []api.NodeInfo) bool {
	resourceRequest := common.TotalResourceRequest(job.PodSpec)
	for _, n := range nodes {
		if fits(resourceRequest, &n) && matchNodeSelector(job, &n) && tolerates(job, &n) {
			return true
		}
	}
	return false
}

func fits(resourceRequest common.ComputeResources, n *api.NodeInfo) bool {
	r := common.ComputeResources(n.AvailableResources).DeepCopy()
	r.Sub(resourceRequest)
	return r.IsValid()
}

func matchNodeSelector(job *api.Job, n *api.NodeInfo) bool {
	for k, v := range job.PodSpec.NodeSelector {
		if n.Labels[k] != v {
			return false
		}
	}
	return true
}

func tolerates(job *api.Job, n *api.NodeInfo) bool {
	for _, taint := range n.Taints {
		// check only hard constraints
		if taint.Effect == v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !tolerationsTolerateTaint(job.PodSpec.Tolerations, &taint) {
			return false
		}
	}
	return true
}

// https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/helpers.go#L427
func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(taint) {
			return true
		}
	}
	return false
}
