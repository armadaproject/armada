package server

import (
	"context"
	"math"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
)

const maxJobsPerLease = 10000

type AggregatedQueueServer struct {
	permissions      authorization.PermissionChecker
	schedulingConfig configuration.SchedulingConfig
	jobRepository    repository.JobRepository
	queueRepository  repository.QueueRepository
	usageRepository  repository.UsageRepository
	eventRepository  repository.EventRepository
	metricRecorder   metrics.MetricRecorder
}

func NewAggregatedQueueServer(
	permissions authorization.PermissionChecker,
	schedulingConfig configuration.SchedulingConfig,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	usageRepository repository.UsageRepository,
	eventRepository repository.EventRepository,
	metricRecorder metrics.MetricRecorder,
) *AggregatedQueueServer {
	return &AggregatedQueueServer{
		permissions:      permissions,
		schedulingConfig: schedulingConfig,
		jobRepository:    jobRepository,
		queueRepository:  queueRepository,
		usageRepository:  usageRepository,
		eventRepository:  eventRepository,
		metricRecorder:   metricRecorder}
}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return &api.JobLease{}, nil
	}

	queues, e := q.queueRepository.GetAllQueues()
	if e != nil {
		return nil, e
	}

	// TODO: doing cleanup here for simplicity, should happen in background loop instead
	expireOldJobs(q.jobRepository, q.eventRepository, queues, 2*time.Minute)

	activeQueues, e := q.jobRepository.FilterActiveQueues(queues)
	if e != nil {
		return nil, e
	}

	usageReports, e := q.usageRepository.GetClusterUsageReports()
	if e != nil {
		return nil, e
	}

	activeClusterReports := scheduling.FilterActiveClusters(usageReports, 10*time.Minute)
	clusterPriorities, e := q.usageRepository.GetClusterPriorities(scheduling.GetClusterReportIds(activeClusterReports))
	if e != nil {
		return nil, e
	}

	resourcesToSchedule := common.ComputeResources(request.Resources).AsFloat()
	currentClusterReport, ok := activeClusterReports[request.ClusterId]
	if ok {
		capacity := common.ComputeResources(currentClusterReport.ClusterCapacity)
		resourcesToSchedule = resourcesToSchedule.LimitWith(capacity.Mul(q.schedulingConfig.MaximalClusterFractionToSchedule))
	}

	queuePriority := scheduling.CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, queues)
	scarcity := scheduling.ResourceScarcityFromReports(activeClusterReports)
	activeQueuePriority := filterPriorityMapByKeys(queuePriority, activeQueues)
	slices := scheduling.SliceResource(scarcity, activeQueuePriority, resourcesToSchedule)

	q.metricRecorder.RecordQueuePriorities(queuePriority)

	jobs := []*api.Job{}
	limit := maxJobsPerLease

	if !q.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		jobs, e = q.assignJobs(ctx, request, slices, limit)
		if e != nil {
			log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterId, e)
			return nil, e
		}
		limit -= len(jobs)
	}

	additionalJobs, e := q.distributeRemainder(ctx, request, scarcity, activeQueuePriority, slices, limit)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterId, e)
		return nil, e
	}
	jobs = append(jobs, additionalJobs...)

	jobLease := api.JobLease{
		Job: jobs,
	}
	if q.schedulingConfig.UseProbabilisticSchedulingForAllResources {
		log.WithField("clusterId", request.ClusterId).Infof("Leasing %d jobs. (using probabilistic scheduling)", len(jobs))
	} else {
		log.WithField("clusterId", request.ClusterId).Infof("Leasing %d jobs. (by remainder distribution: %d)", len(jobs), len(additionalJobs))
	}
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
	jobs, e := q.jobRepository.GetExistingJobsByIds(idList.Ids)
	if e != nil {
		return nil, status.Errorf(codes.Internal, e.Error())
	}
	deletionResult := q.jobRepository.DeleteJobs(jobs)

	cleanedIds := make([]string, 0, len(deletionResult))
	var returnedError error = nil
	for job, err := range deletionResult {
		if err != nil {
			returnedError = err
		} else {
			cleanedIds = append(cleanedIds, job.Id)
		}
	}
	return &api.IdList{cleanedIds}, returnedError
}

func (q *AggregatedQueueServer) assignJobs(ctx context.Context, request *api.LeaseRequest, slices map[*api.Queue]common.ComputeResourcesFloat, limit int) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	// TODO: parallelize
	for queue, slice := range slices {
		// TODO: partition limit by priority instead
		leased, remainder, e := q.leaseJobs(ctx, request, queue, slice, limit/len(slices))
		if e != nil {
			log.Error(e)
			continue
		}
		slices[queue] = remainder
		jobs = append(jobs, leased...)

		if closeToDeadline(ctx) {
			break
		}
	}
	return jobs, nil
}

func (q *AggregatedQueueServer) distributeRemainder(ctx context.Context, request *api.LeaseRequest, resourceScarcity map[string]float64, priorities map[*api.Queue]scheduling.QueuePriorityInfo, slices map[*api.Queue]common.ComputeResourcesFloat, limit int) ([]*api.Job, error) {
	jobs := []*api.Job{}
	if limit <= 0 {
		return jobs, nil
	}

	remainder := common.ComputeResourcesFloat{}
	shares := map[*api.Queue]float64{}
	for queue, slice := range slices {
		remainder.Add(slice)
		shares[queue] = scheduling.ResourcesFloatAsUsage(resourceScarcity, slice)
	}

	queueCount := len(slices)
	emptySteps := 0
	minimumResource := q.schedulingConfig.MinimumResourceToSchedule
	for !remainder.IsLessThan(minimumResource) && emptySteps < queueCount {
		queue := q.pickQueueRandomly(shares)
		emptySteps++

		leased, remaining, e := q.leaseJobs(ctx, request, queue, remainder, 1)
		if e != nil {
			log.Error(e)
			continue
		}
		if len(leased) > 0 {
			emptySteps = 0
		}
		jobs = append(jobs, leased...)
		scheduledShare := scheduling.ResourcesFloatAsUsage(resourceScarcity, remainder) - scheduling.ResourcesFloatAsUsage(resourceScarcity, remaining)
		shares[queue] = math.Max(0, shares[queue]-scheduledShare)
		remainder = remaining

		limit -= len(leased)
		if limit <= 0 || closeToDeadline(ctx) {
			break
		}
	}

	return jobs, nil
}

func (q *AggregatedQueueServer) pickQueueRandomly(shares map[*api.Queue]float64) *api.Queue {
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

func (q *AggregatedQueueServer) leaseJobs(ctx context.Context, request *api.LeaseRequest, queue *api.Queue, slice common.ComputeResourcesFloat, limit int) ([]*api.Job, common.ComputeResourcesFloat, error) {
	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {
		if limit <= 0 {
			break
		}

		topJobs, e := q.jobRepository.PeekQueue(queue.Name, int64(q.schedulingConfig.QueueLeaseBatchSize))
		if e != nil {
			return nil, slice, e
		}

		candidates := make([]*api.Job, 0)
		for _, job := range topJobs {
			requirement := common.TotalResourceRequest(job.PodSpec).AsFloat()
			remainder = slice.DeepCopy()
			remainder.Sub(requirement)
			if remainder.IsValid() && matchRequirements(job, request) {
				slice = remainder
				candidates = append(candidates, job)
			}
			if len(candidates) >= limit {
				break
			}
		}

		leased, e := q.jobRepository.TryLeaseJobs(request.ClusterId, queue.Name, candidates)
		if e != nil {
			return nil, slice, e
		}

		jobs = append(jobs, leased...)
		limit -= len(leased)

		// stop scheduling round if we leased less then batch (either the slice is too small or queue is empty)
		// TODO: should we look at next batch?
		if len(candidates) < int(q.schedulingConfig.QueueLeaseBatchSize) {
			break
		}
		if closeToDeadline(ctx) {
			break
		}
	}

	go reportJobsLeased(q.eventRepository, jobs, request.ClusterId)

	return jobs, slice, nil
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

func filterPriorityMapByKeys(original map[*api.Queue]scheduling.QueuePriorityInfo, keys []*api.Queue) map[*api.Queue]scheduling.QueuePriorityInfo {
	result := make(map[*api.Queue]scheduling.QueuePriorityInfo)
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

func closeToDeadline(ctx context.Context) bool {
	d, exists := ctx.Deadline()
	return exists && d.Before(time.Now().Add(time.Second))
}
