package cache

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

const objectsToLoadBatchSize = 10000

type (
	empty     struct{}
	stringSet map[string]empty
)

type QueueCache struct {
	clock                    util.Clock
	queueRepository          repository.QueueRepository
	jobRepository            repository.JobRepository
	schedulingInfoRepository repository.SchedulingInfoRepository

	refreshMutex     sync.Mutex
	queuedDurations  map[string]map[string]*metrics.FloatMetrics
	queuedResources  map[string]map[string]metrics.ResourceMetrics
	runningDurations map[string]map[string]*metrics.FloatMetrics
	runningResources map[string]map[string]metrics.ResourceMetrics

	queueNonMatchingJobIds map[string]map[string]stringSet
}

func NewQueueCache(
	clock util.Clock,
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	schedulingInfoRepository repository.SchedulingInfoRepository,
) *QueueCache {
	collector := &QueueCache{
		clock:                    clock,
		queueRepository:          queueRepository,
		jobRepository:            jobRepository,
		schedulingInfoRepository: schedulingInfoRepository,
		queuedDurations:          map[string]map[string]*metrics.FloatMetrics{},
		queuedResources:          map[string]map[string]metrics.ResourceMetrics{},
		queueNonMatchingJobIds:   map[string]map[string]stringSet{},
		runningDurations:         map[string]map[string]*metrics.FloatMetrics{},
		runningResources:         map[string]map[string]metrics.ResourceMetrics{},
	}

	return collector
}

func (c *QueueCache) Refresh() {
	queues, e := c.queueRepository.GetAllQueues()
	if e != nil {
		log.Errorf("Error while getting queues %s", e)
		return
	}

	clusterInfo, e := c.schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		log.Errorf("Error while getting cluster reports %s", e)
		return
	}

	activeClusterInfo := scheduling.FilterActiveClusterSchedulingInfoReports(clusterInfo)
	clusterInfoByPool := scheduling.GroupSchedulingInfoByPool(activeClusterInfo)

	for _, q := range queues {
		e := c.calculateRunningJobMetrics(q, activeClusterInfo)
		if e != nil {
			log.Errorf("Failed calculating running jobs metrics for queue %s because %s", q.Name, e)
			continue
		}

		e = c.calculateQueuedJobMetrics(q, clusterInfoByPool)
		if e != nil {
			log.Errorf("Failed calculating queued job metrics for queue %s because %s", q.Name, e)
			continue
		}
	}
}

func (c *QueueCache) calculateQueuedJobMetrics(queue queue.Queue, clusterInfoByPool map[string]map[string]*api.ClusterSchedulingInfoReport) error {
	queuedJobIds, e := c.jobRepository.GetQueueJobIds(queue.Name)
	if e != nil {
		return fmt.Errorf("failed getting queued jobs - %s", e)
	}

	resourceUsageByPool := map[string]*metrics.ResourceMetricsRecorder{}
	nonMatchingJobs := map[string]stringSet{}
	queueDurationByPool := map[string]*metrics.FloatMetricsRecorder{}
	currentTime := c.clock.Now()
	for _, chunkJobIds := range util.Batch(queuedJobIds, objectsToLoadBatchSize) {
		queuedJobs, e := c.jobRepository.GetExistingJobsByIds(chunkJobIds)
		if e != nil {
			return fmt.Errorf("failed loading jobs - %s", e)
		}

		for _, job := range queuedJobs {
			jobResources := common.TotalJobResourceRequest(job)
			nonMatchingClusters := stringSet{}
			queuedTime := currentTime.Sub(job.Created)

			for pool, infos := range clusterInfoByPool {
				matches := false
				for _, schedulingInfo := range infos {
					if ok, _ := scheduling.MatchSchedulingRequirements(job, schedulingInfo); ok {
						matches = true
					} else {
						nonMatchingClusters[schedulingInfo.ClusterId] = empty{}
					}
				}

				if matches {
					r, exists := resourceUsageByPool[pool]
					if !exists {
						r = metrics.NewResourceMetricsRecorder()
						resourceUsageByPool[pool] = r
					}
					r.Record(jobResources.AsFloat())

					qd, exists := queueDurationByPool[pool]
					if !exists {
						qd = metrics.NewDefaultJobDurationMetricsRecorder()
						queueDurationByPool[pool] = qd
					}
					qd.Record(queuedTime.Seconds())
				}
			}
			nonMatchingJobs[job.Id] = nonMatchingClusters
		}
	}

	c.updateQueuedNonMatchingJobs(queue.Name, nonMatchingJobs)
	c.updateQueueMetrics(queue.Name, resourceUsageByPool, queueDurationByPool)
	return nil
}

func (c *QueueCache) calculateRunningJobMetrics(queue queue.Queue, activeClusterInfos map[string]*api.ClusterSchedulingInfoReport) error {
	clusterIdToPool := map[string]string{}
	for _, clusterInfo := range activeClusterInfos {
		clusterIdToPool[clusterInfo.ClusterId] = clusterInfo.Pool
	}

	durationMetricsRecorderByPool := make(map[string]*metrics.FloatMetricsRecorder)
	resourceMetricsRecorderByPool := make(map[string]*metrics.ResourceMetricsRecorder)
	leasedJobsIds, e := c.jobRepository.GetLeasedJobIds(queue.Name)
	if e != nil {
		return fmt.Errorf("failed getting lease job ids - %s", e)
	}

	for _, chunkJobIds := range util.Batch(leasedJobsIds, objectsToLoadBatchSize) {
		leasedJobs, e := c.jobRepository.GetExistingJobsByIds(chunkJobIds)
		if e != nil {
			return fmt.Errorf("failed getting leased jobs - %s", e)
		}

		runInfo, e := c.jobRepository.GetJobRunInfos(chunkJobIds)
		if e != nil {
			return fmt.Errorf("failed getting job run info - %s", e)
		}
		now := c.clock.Now()
		for _, job := range leasedJobs {
			runInfo, present := runInfo[job.Id]
			if !present {
				continue
			}
			pool, present := clusterIdToPool[runInfo.CurrentClusterId]
			if !present {
				continue
			}
			jobResources := common.TotalJobResourceRequest(job)
			runTime := now.Sub(runInfo.StartTime)

			r, exists := durationMetricsRecorderByPool[pool]
			if !exists {
				r = metrics.NewDefaultJobDurationMetricsRecorder()
				durationMetricsRecorderByPool[pool] = r
			}
			r.Record(runTime.Seconds())

			resource, exists := resourceMetricsRecorderByPool[pool]
			if !exists {
				resource = metrics.NewResourceMetricsRecorder()
				resourceMetricsRecorderByPool[pool] = resource
			}
			resource.Record(jobResources.AsFloat())
		}
	}

	c.updateRunningMetrics(queue.Name, resourceMetricsRecorderByPool, durationMetricsRecorderByPool)
	return nil
}

func (c *QueueCache) updateQueueMetrics(queueName string, resourcesByPool map[string]*metrics.ResourceMetricsRecorder,
	queueDurationsByPool map[string]*metrics.FloatMetricsRecorder,
) {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()

	durationMetricsByPool := make(map[string]*metrics.FloatMetrics, len(queueDurationsByPool))
	for pool, queueDurations := range queueDurationsByPool {
		durationMetricsByPool[pool] = queueDurations.GetMetrics()
	}
	c.queuedDurations[queueName] = durationMetricsByPool

	resourceMetricsByPool := make(map[string]metrics.ResourceMetrics, len(resourcesByPool))
	for pool, res := range resourcesByPool {
		resourceMetricsByPool[pool] = res.GetMetrics()
	}
	c.queuedResources[queueName] = resourceMetricsByPool
}

func (c *QueueCache) updateRunningMetrics(queueName string, resourcesByPool map[string]*metrics.ResourceMetricsRecorder,
	runningJobDurationsByPool map[string]*metrics.FloatMetricsRecorder,
) {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()

	durationMetricsByPool := make(map[string]*metrics.FloatMetrics, len(runningJobDurationsByPool))
	for pool, queueDurations := range runningJobDurationsByPool {
		durationMetricsByPool[pool] = queueDurations.GetMetrics()
	}
	c.runningDurations[queueName] = durationMetricsByPool

	resourceMetricsByPool := make(map[string]metrics.ResourceMetrics, len(resourcesByPool))
	for pool, res := range resourcesByPool {
		resourceMetricsByPool[pool] = res.GetMetrics()
	}
	c.runningResources[queueName] = resourceMetricsByPool
}

func (c *QueueCache) updateQueuedNonMatchingJobs(queueName string, nonMatchingClustersById map[string]stringSet) {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	c.queueNonMatchingJobIds[queueName] = nonMatchingClustersById
}

func (c *QueueCache) GetQueuedJobMetrics(queueName string) *metrics.QueueMetrics {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	return &metrics.QueueMetrics{
		Resources: c.queuedResources[queueName],
		Durations: c.queuedDurations[queueName],
	}
}

func (c *QueueCache) GetRunningJobMetrics(queueName string) *metrics.QueueMetrics {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	return &metrics.QueueMetrics{
		Resources: c.runningResources[queueName],
		Durations: c.runningDurations[queueName],
	}
}

func (c *QueueCache) getNonSchedulableJobIds(queueName string) map[string]stringSet {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	return c.queueNonMatchingJobIds[queueName]
}

func (c *QueueCache) PeekClusterQueue(clusterId, queue string, limit int64) ([]*api.Job, error) {
	ids, e := c.jobRepository.GetQueueJobIds(queue)
	if e != nil {
		return nil, e
	}
	nonMatchingJobs := c.getNonSchedulableJobIds(queue)

	filtered := []string{}
	for _, id := range ids {
		if matches(nonMatchingJobs, clusterId, id) {
			filtered = append(filtered, id)
		}
		if len(filtered) == int(limit) {
			break
		}
	}
	return c.jobRepository.GetExistingJobsByIds(filtered)
}

func matches(nonMatchingJobs map[string]stringSet, clusterId, jobId string) bool {
	nonMatchingClusters, ok := nonMatchingJobs[jobId]
	if !ok {
		return true
	}
	_, exists := nonMatchingClusters[clusterId]
	return !exists
}

func (c *QueueCache) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	return c.jobRepository.TryLeaseJobs(clusterId, queue, jobs)
}
