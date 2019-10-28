package server

import (
	"context"
	"sort"
	"time"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/service"
	types2 "github.com/G-Research/armada/internal/armada/types"
	"github.com/G-Research/armada/internal/common"
)

type AggregatedQueueServer struct {
	permissions     authorization.PermissionChecker
	priorityService service.PriorityService
	jobRepository   repository.JobRepository
	eventRepository repository.EventRepository
}

func NewAggregatedQueueServer(
	permissions authorization.PermissionChecker,
	priorityService service.PriorityService,
	jobRepository repository.JobRepository,
	eventRepository repository.EventRepository,
) *AggregatedQueueServer {

	return &AggregatedQueueServer{
		permissions:     permissions,
		priorityService: priorityService,
		jobRepository:   jobRepository,
		eventRepository: eventRepository}
}

const batchSize = 100

var minimalResource = common.ComputeResourcesFloat{"cpu": 0.25, "memory": 100.0 * 1024 * 1024}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(minimalResource) {
		return &api.JobLease{}, nil
	}

	queuePriority, e := q.priorityService.GetQueuePriorities()
	if e != nil {
		return nil, e
	}
	queues := types2.GetPriorityMapQueues(queuePriority)

	// TODO: doing cleanup here for simplicity, should happen in background loop instead
	expireOldJobs(q.jobRepository, q.eventRepository, queues, 2*time.Minute)

	activeQueues, e := q.jobRepository.FilterActiveQueues(queues)
	if e != nil {
		return nil, e
	}
	activeQueuePriority := filterPriorityMapByKeys(queuePriority, activeQueues)

	slices := sliceResource(activeQueuePriority, request.Resources)

	jobs, e := q.assignJobs(request.ClusterId, slices)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterId, e)
		return nil, e
	}
	additionalJobs, e := q.distributeRemainder(request.ClusterId, activeQueuePriority, slices)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterId, e)
		return nil, e
	}
	jobs = append(jobs, additionalJobs...)

	jobLease := api.JobLease{
		Job: jobs,
	}
	log.WithField("clusterId", request.ClusterId).Infof("Leasing %d jobs.", len(jobs))
	return &jobLease, nil
}

func (q *AggregatedQueueServer) RenewLease(ctx context.Context, request *api.RenewLeaseRequest) (*api.IdList, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	renewed, e := q.jobRepository.RenewLease(request.ClusterId, request.Ids)
	return &api.IdList{renewed}, e
}

func (q *AggregatedQueueServer) ReturnLease(ctx context.Context, request *api.ReturnLeaseRequest) (*types.Empty, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	_, err := q.jobRepository.ReturnLease(request.ClusterId, request.JobId)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (q *AggregatedQueueServer) ReportDone(ctx context.Context, idList *api.IdList) (*api.IdList, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	cleaned, e := q.jobRepository.Remove(idList.Ids)
	return &api.IdList{cleaned}, e
}

func (q *AggregatedQueueServer) assignJobs(clusterId string, slices map[*api.Queue]common.ComputeResourcesFloat) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, slice := range slices {
		leased, remainder, e := q.leaseJobs(clusterId, queue, slice, -1)
		if e != nil {
			log.Error(e)
			continue
		}
		slices[queue] = remainder
		jobs = append(jobs, leased...)
	}
	return jobs, nil
}

func (q *AggregatedQueueServer) distributeRemainder(clusterId string, priorities map[*api.Queue]types2.QueuePriorityInfo, slices map[*api.Queue]common.ComputeResourcesFloat) ([]*api.Job, error) {
	jobs := []*api.Job{}
	remainder := common.ComputeResourcesFloat{}
	orderedQueues := []*api.Queue{}
	for queue, slice := range slices {
		remainder.Add(slice)
		orderedQueues = append(orderedQueues, queue)
	}
	sort.Slice(orderedQueues, func(i, j int) bool {
		return priorities[orderedQueues[i]].Priority < priorities[orderedQueues[j]].Priority
	})

	for _, queue := range orderedQueues {
		leased, remaining, e := q.leaseJobs(clusterId, queue, remainder, 1)
		if e != nil {
			log.Error(e)
			continue
		}
		jobs = append(jobs, leased...)
		remainder = remaining
		if remainder.IsLessThan(minimalResource) {
			break
		}
	}

	return jobs, nil
}

func (q *AggregatedQueueServer) leaseJobs(clusterId string, queue *api.Queue, slice common.ComputeResourcesFloat, limit int) ([]*api.Job, common.ComputeResourcesFloat, error) {
	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {

		topJobs, e := q.jobRepository.PeekQueue(queue.Name, batchSize)
		if e != nil {
			return nil, slice, e
		}

		candidates := make([]*api.Job, 0)
		for _, job := range topJobs {
			requirement := common.TotalResourceRequest(job.PodSpec).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if remainder.IsValid() {
				slice = remainder
				candidates = append(candidates, job)
			}
			if limit > 0 && len(candidates) >= limit {
				break
			}
		}

		leased, e := q.jobRepository.TryLeaseJobs(clusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)

		// stop scheduling round if we leased less then batch (either the slice is too small or queue is empty)
		// TODO: should we look at next batch?
		if len(candidates) < batchSize {
			break
		}
		if limit > 0 && len(candidates) >= limit {
			break
		}
	}

	go reportJobsLeased(q.eventRepository, jobs, clusterId)

	return jobs, slice, nil
}

func sliceResource(queuePriorities map[*api.Queue]types2.QueuePriorityInfo, quantityToSlice common.ComputeResources) map[*api.Queue]common.ComputeResourcesFloat {

	inversePriority := make(map[*api.Queue]float64)
	inverseSum := 0.0
	resourcesInUse := common.ComputeResources{}
	for queue, info := range queuePriorities {
		inverse := 1 / info.Priority
		inversePriority[queue] = inverse
		inverseSum += inverse
		resourcesInUse.Add(info.CurrentUsage)
	}

	allResources := resourcesInUse.DeepCopy()
	allResources.Add(quantityToSlice)

	shares := make(map[*api.Queue]common.ComputeResourcesFloat)
	for queue, inverse := range inversePriority {
		share := allResources.Mul(inverse / inverseSum)
		share.Sub(queuePriorities[queue].CurrentUsage.AsFloat())
		shares[queue] = share
	}
	return shares
}

func filterPriorityMapByKeys(original map[*api.Queue]types2.QueuePriorityInfo, keys []*api.Queue) map[*api.Queue]types2.QueuePriorityInfo {
	result := make(map[*api.Queue]types2.QueuePriorityInfo)
	for _, key := range keys {
		existing, ok := original[key]
		if ok {
			result[key] = existing
		}
	}
	return result
}

func expireOldJobs(jobRepository repository.JobRepository, eventRepository repository.EventRepository, queues []*api.Queue, expiryInterval time.Duration) {
	deadline := time.Now().Add(-expiryInterval)
	for _, queue := range queues {
		jobs, e := jobRepository.ExpireLeases(queue.Name, deadline)
		now := time.Now()
		if e != nil {
			log.Error(e)
		} else {
			for _, job := range jobs {
				event, e := api.Wrap(&api.JobLeaseExpiredEvent{
					JobId:    job.Id,
					Queue:    job.Queue,
					JobSetId: job.JobSetId,
					Created:  now,
				})
				if e != nil {
					log.Error(e)
				} else {
					e := eventRepository.ReportEvent(event)
					if e != nil {
						log.Error(e)
					}
				}
			}
		}
	}
}
