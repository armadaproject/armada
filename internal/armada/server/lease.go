package server

import (
	"context"
	"sort"
	"time"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/armada/service"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/common/util"
)

type AggregatedQueueServer struct {
	priorityService service.PriorityService
	jobRepository   repository.JobRepository
	eventRepository repository.EventRepository
}

func NewAggregatedQueueServer(
	priorityService service.PriorityService,
	jobRepository repository.JobRepository,
	eventRepository repository.EventRepository,
) *AggregatedQueueServer {

	return &AggregatedQueueServer{
		priorityService: priorityService,
		jobRepository:   jobRepository,
		eventRepository: eventRepository}
}

const batchSize = 100

var minimalResource = common.ComputeResourcesFloat{"cpu": 0.25, "memory": 100.0 * 1024 * 1024}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(minimalResource) {
		return &api.JobLease{}, nil
	}

	queuePriority, e := q.priorityService.GetQueuePriorities()
	queues := util.GetPriorityMapQueues(queuePriority)

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

func (q AggregatedQueueServer) RenewLease(ctx context.Context, request *api.RenewLeaseRequest) (*api.IdList, error) {
	renewed, e := q.jobRepository.RenewLease(request.ClusterId, request.Ids)
	return &api.IdList{renewed}, e
}

func (q AggregatedQueueServer) ReturnLease(ctx context.Context, request *api.ReturnLeaseRequest) (*types.Empty, error) {
	returnedJob, err := q.jobRepository.ReturnLease(request.ClusterId, request.JobId)
	if err != nil {
		return nil, err
	}
	if returnedJob != nil {
		event, err := api.Wrap(&api.JobLeaseReturnedEvent{
			JobId:      returnedJob.Id,
			Queue:      returnedJob.Queue,
			JobSetId:   returnedJob.JobSetId,
			Created:    time.Now(),
			Reason:     request.Reason,
			ReasonType: request.ReasonType,
			ClusterId:  request.ClusterId,
		})

		err = q.eventRepository.ReportEvent(event)
		if err != nil {
			return nil, err
		}
	}
	return &types.Empty{}, nil
}

func (q AggregatedQueueServer) ReportDone(ctx context.Context, idList *api.IdList) (*api.IdList, error) {
	cleaned, e := q.jobRepository.Remove(idList.Ids)
	return &api.IdList{cleaned}, e
}

func (q AggregatedQueueServer) assignJobs(clusterId string, slices map[*api.Queue]common.ComputeResourcesFloat) ([]*api.Job, error) {
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

func (q AggregatedQueueServer) distributeRemainder(clusterId string, priorities map[*api.Queue]float64, slices map[*api.Queue]common.ComputeResourcesFloat) ([]*api.Job, error) {
	jobs := []*api.Job{}
	remainder := common.ComputeResourcesFloat{}
	orderedQueues := []*api.Queue{}
	for queue, slice := range slices {
		remainder.Add(slice)
		orderedQueues = append(orderedQueues, queue)
	}
	sort.Slice(orderedQueues, func(i, j int) bool {
		return priorities[orderedQueues[i]] < priorities[orderedQueues[j]]
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

func (q AggregatedQueueServer) leaseJobs(clusterId string, queue *api.Queue, slice common.ComputeResourcesFloat, limit int) ([]*api.Job, common.ComputeResourcesFloat, error) {
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

func sliceResource(queuePriorities map[*api.Queue]float64, quantity common.ComputeResources) map[*api.Queue]common.ComputeResourcesFloat {
	inversePriority := make(map[*api.Queue]float64)
	for queue, priority := range queuePriorities {
		inversePriority[queue] = 1 / priority
	}
	inverseSum := 0.0
	for _, inverse := range inversePriority {
		inverseSum += inverse
	}

	shares := make(map[*api.Queue]common.ComputeResourcesFloat)
	for queue, inverse := range inversePriority {
		shares[queue] = quantity.Mul(inverse / inverseSum)
	}
	return shares
}

func filterPriorityMapByKeys(original map[*api.Queue]float64, keys []*api.Queue) map[*api.Queue]float64 {
	result := make(map[*api.Queue]float64)
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
