package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/metrics"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/common/util"
	log "github.com/sirupsen/logrus"
	"math"
	"sort"
	"time"
)

type AggregatedQueueServer struct {
	jobRepository   repository.JobRepository
	usageRepository repository.UsageRepository
	queueRepository repository.QueueRepository
	eventRepository repository.EventRepository
	metricRecorder  metrics.MetricRecorder
}

func NewAggregatedQueueServer(
	jobRepository repository.JobRepository,
	usageRepository repository.UsageRepository,
	queueRepository repository.QueueRepository,
	eventRepository repository.EventRepository,
	metricRecorder metrics.MetricRecorder,
) *AggregatedQueueServer {

	return &AggregatedQueueServer{
		jobRepository:   jobRepository,
		usageRepository: usageRepository,
		queueRepository: queueRepository,
		eventRepository: eventRepository,
		metricRecorder:  metricRecorder}
}

const minPriority = 0.5
const batchSize = 100

var minimalResource = common.ComputeResourcesFloat{"cpu": 0.25, "memory": 100.0 * 1024 * 1024}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(minimalResource) {
		return &api.JobLease{}, nil
	}

	queuePriority, e := q.calculatePriorities()
	if e != nil {
		return nil, e
	}
	q.metricRecorder.RecordQueuePriorities(queuePriority)

	queues, e := q.queueRepository.GetQueues()
	if e != nil {
		return nil, e
	}

	// TODO: doing cleanup here for simplicity, should happen in background loop instead
	expireOldJobs(q.jobRepository, q.eventRepository, queues, 2*time.Minute)

	activeQueues, e := q.jobRepository.FilterActiveQueues(queues)
	if e != nil {
		return nil, e
	}
	activeQueueNames := getQueueNames(activeQueues)
	activeQueuePriority := filterMapByKeys(queuePriority, activeQueueNames, minPriority)
	slices := sliceResource(activeQueuePriority, request.Resources)
	jobs, e := q.assignJobs(request.ClusterID, slices)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterID, e)
		return nil, e
	}
	additionalJobs, e := q.distributeRemainder(request.ClusterID, activeQueuePriority, slices)
	if e != nil {
		log.Errorf("Error when leasing jobs for cluster %s: %s", request.ClusterID, e)
		return nil, e
	}
	jobs = append(jobs, additionalJobs...)

	jobLease := api.JobLease{
		Job: jobs,
	}
	log.WithField("clusterId", request.ClusterID).Infof("Leasing %d jobs.", len(jobs))
	return &jobLease, nil
}

func (q AggregatedQueueServer) RenewLease(ctx context.Context, request *api.RenewLeaseRequest) (*api.IdList, error) {
	renewed, e := q.jobRepository.RenewLease(request.ClusterID, request.Ids)
	return &api.IdList{renewed}, e
}

func (q AggregatedQueueServer) ReportDone(ctx context.Context, idList *api.IdList) (*api.IdList, error) {
	cleaned, e := q.jobRepository.Remove(idList.Ids)
	return &api.IdList{cleaned}, e
}

func (q AggregatedQueueServer) assignJobs(clusterId string, slices map[string]common.ComputeResourcesFloat) ([]*api.Job, error) {
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

func (q AggregatedQueueServer) distributeRemainder(clusterId string, priorities map[string]float64, slices map[string]common.ComputeResourcesFloat) ([]*api.Job, error) {
	jobs := []*api.Job{}
	remainder := common.ComputeResourcesFloat{}
	orderedQueues := []string{}
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

func (q AggregatedQueueServer) calculatePriorities() (map[string]float64, error) {

	usageReports, e := q.usageRepository.GetClusterUsageReports()
	if e != nil {
		return nil, e
	}

	activeClusterIds := filterActiveClusters(usageReports, 10*time.Minute)
	clusterPriorities, e := q.usageRepository.GetClusterPriorities(activeClusterIds)
	if e != nil {
		return nil, e
	}
	queuePriority := aggregatePriority(clusterPriorities)

	return queuePriority, nil
}

func (q AggregatedQueueServer) leaseJobs(clusterId string, queue string, slice common.ComputeResourcesFloat, limit int) ([]*api.Job, common.ComputeResourcesFloat, error) {
	jobs := make([]*api.Job, 0)
	remainder := slice
	for slice.IsValid() {

		topJobs, e := q.jobRepository.PeekQueue(queue, batchSize)
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

		leased, e := q.jobRepository.TryLeaseJobs(clusterId, queue, candidates)
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

func sliceResource(queuePriorities map[string]float64, quantity common.ComputeResources) map[string]common.ComputeResourcesFloat {
	inversePriority := make(map[string]float64)
	for queue, priority := range queuePriorities {
		inversePriority[queue] = 1 / math.Max(priority, minPriority)
	}
	inverseSum := 0.0
	for _, inverse := range inversePriority {
		inverseSum += inverse
	}

	shares := make(map[string]common.ComputeResourcesFloat)
	for queue, inverse := range inversePriority {
		shares[queue] = quantity.Mul(inverse / inverseSum)
	}
	return shares
}

func filterActiveClusters(reports map[string]*api.ClusterUsageReport, expiry time.Duration) []string {
	var result []string
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(expiry).After(now) {
			result = append(result, id)
		}
	}
	return result
}

func aggregatePriority(clusterPriorities map[string]map[string]float64) map[string]float64 {
	result := make(map[string]float64)
	for _, clusterPriority := range clusterPriorities {
		for queue, priority := range clusterPriority {
			result[queue] = priority + util.GetOrDefault(result, queue, 0)
		}
	}
	return result
}

func getQueueNames(queues []*api.Queue) []string {
	result := make([]string, 0)
	for _, q := range queues {
		result = append(result, q.Name)
	}
	return result
}

func filterMapByKeys(original map[string]float64, keys []string, defaultValue float64) map[string]float64 {
	result := make(map[string]float64)
	for _, key := range keys {
		result[key] = util.GetOrDefault(original, key, defaultValue)
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
				event, e := api.Wrap(&api.JobLeaseExpired{
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
