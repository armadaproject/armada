package scheduling

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common"

	log "github.com/sirupsen/logrus"
)

const maxJobsPerLease = 10000

type leaseContext struct {
	schedulingConfig *configuration.SchedulingConfig
	repository       repository.JobQueueRepository
	onJobsLeased     func([]*api.Job)

	ctx     context.Context
	request *api.LeaseRequest

	slices           map[*api.Queue]common.ComputeResourcesFloat
	resourceScarcity map[string]float64
	priorities       map[*api.Queue]QueuePriorityInfo

	queueCache map[string][]*api.Job
}

func LeaseJobs(
	ctx context.Context,
	config *configuration.SchedulingConfig,
	jobQueueRepository repository.JobQueueRepository,
	onJobLease func([]*api.Job),
	request *api.LeaseRequest,
	activeClusterReports map[string]*api.ClusterUsageReport,
	clusterPriorities map[string]map[string]float64,
	queues []*api.Queue,
	activeQueues []*api.Queue,
) ([]*api.Job, error) {
	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]

	if ok {
		capacity := common.ComputeResources(currentClusterReport.ClusterCapacity)
		resourcesToSchedule = resourcesToSchedule.LimitWith(capacity.MulByResource(config.MaximalClusterFractionToSchedule))
	}

	queuePriority := CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, queues)
	scarcity := ResourceScarcityFromReports(activeClusterReports)
	activeQueuePriority := filterPriorityMapByKeys(queuePriority, activeQueues)
	slices := SliceResource(scarcity, activeQueuePriority, resourcesToSchedule)

	limit := maxJobsPerLease
	lc := &leaseContext{
		schedulingConfig: config,
		repository:       jobQueueRepository,

		ctx:     ctx,
		request: request,

		slices:           slices,
		resourceScarcity: scarcity,
		priorities:       activeQueuePriority,

		queueCache: map[string][]*api.Job{},

		onJobsLeased: onJobLease,
	}

	return lc.scheduleJobs(limit)
}

func (c *leaseContext) scheduleJobs(limit int) ([]*api.Job, error) {
	jobs := []*api.Job{}

	if !c.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		jobs, e := c.assignJobs(limit)
		if e != nil {
			log.Errorf("Error when leasing jobs for cluster %s: %s", c.request.ClusterId, e)
			return nil, e
		}
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
	for queue, slice := range c.slices {
		// TODO: partition limit by priority instead
		leased, remainder, e := c.leaseJobs(queue, slice, limit/len(c.slices))
		if e != nil {
			log.Error(e)
			continue
		}
		c.slices[queue] = remainder
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

	remainder := common.ComputeResourcesFloat{}
	shares := map[*api.Queue]float64{}
	for queue, slice := range c.slices {
		remainder.Add(slice)
		shares[queue] = ResourcesFloatAsUsage(c.resourceScarcity, slice)
	}

	queueCount := len(c.slices)
	emptySteps := 0
	minimumResource := c.schedulingConfig.MinimumResourceToSchedule

	for !remainder.IsLessThan(minimumResource) && emptySteps < queueCount {
		queue := pickQueueRandomly(shares)
		emptySteps++

		leased, remaining, e := c.leaseJobs(queue, remainder, 1)
		if e != nil {
			log.Error(e)
			continue
		}
		if len(leased) > 0 {
			emptySteps = 0
			jobs = append(jobs, leased...)
			scheduledShare := ResourcesFloatAsUsage(c.resourceScarcity, remainder) - ResourcesFloatAsUsage(c.resourceScarcity, remaining)
			shares[queue] = math.Max(0, shares[queue]-scheduledShare)
			remainder = remaining
		} else {
			// if there are no suitable jobs to lease eliminate queue from the scheduling
			shares[queue] = 0
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
			topJobs = newTop
		}

		candidates := make([]*api.Job, 0)
		for i, job := range topJobs {
			requirement := common.TotalResourceRequest(job.PodSpec).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if remainder.IsValid() && matchRequirements(job, c.request) {
				slice = remainder
				candidates = append(candidates, job)

				c.queueCache[queue.Name] = append(topJobs[:i], topJobs[i+1:]...)
			}
			if len(candidates) >= limit {
				break
			}
		}

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

func matchRequirements(job *api.Job, request *api.LeaseRequest) bool {
	if len(job.RequiredNodeLabels) == 0 {
		return true
	}

Labels:
	for _, labeling := range request.AvailableLabels {
		for k, v := range job.RequiredNodeLabels {
			if labeling.Labels[k] != v {
				continue Labels
			}
		}
		return true
	}
	return false
}

func filterPriorityMapByKeys(original map[*api.Queue]QueuePriorityInfo, keys []*api.Queue) map[*api.Queue]QueuePriorityInfo {
	result := make(map[*api.Queue]QueuePriorityInfo)
	for _, key := range keys {
		existing, ok := original[key]
		if ok {
			result[key] = existing
		}
	}
	return result
}
