package cache

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/metrics"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
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

	refreshMutex      sync.Mutex
	queuedJobMetrics  map[string][]*metrics.QueueMetrics
	runningJobMetrics map[string][]*metrics.QueueMetrics

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
		queuedJobMetrics:         map[string][]*metrics.QueueMetrics{},
		runningJobMetrics:        map[string][]*metrics.QueueMetrics{},
		queueNonMatchingJobIds:   map[string]map[string]stringSet{},
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

func (c *QueueCache) calculateQueuedJobMetrics(
	queue queue.Queue,
	clusterInfoByPool map[string]map[string]*api.ClusterSchedulingInfoReport,
) error {
	queuedJobIds, e := c.jobRepository.GetQueueJobIds(queue.Name)
	if e != nil {
		return fmt.Errorf("failed getting queued jobs - %s", e)
	}

	metricsRecorder := metrics.NewJobMetricsRecorder()
	nonMatchingJobs := map[string]stringSet{}
	currentTime := c.clock.Now()
	for _, chunkJobIds := range util.Batch(queuedJobIds, objectsToLoadBatchSize) {
		queuedJobs, e := c.jobRepository.GetExistingJobsByIds(chunkJobIds)
		if e != nil {
			return fmt.Errorf("failed loading jobs - %s", e)
		}

		for _, job := range queuedJobs {
			jobResources := job.TotalResourceRequest()
			nonMatchingClusters := stringSet{}
			queuedTime := currentTime.Sub(job.Created)

			priorityClass := getPriorityClass(job)

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
					metricsRecorder.RecordResources(pool, priorityClass, jobResources.AsFloat())
					metricsRecorder.RecordJobRuntime(pool, priorityClass, queuedTime)
				}
			}
			nonMatchingJobs[job.Id] = nonMatchingClusters
		}
	}

	c.updateQueuedNonMatchingJobs(queue.Name, nonMatchingJobs)
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	c.queuedJobMetrics[queue.Name] = metricsRecorder.Metrics()
	return nil
}

func (c *QueueCache) calculateRunningJobMetrics(queue queue.Queue, activeClusterInfos map[string]*api.ClusterSchedulingInfoReport) error {
	clusterIdToPool := map[string]string{}
	for _, clusterInfo := range activeClusterInfos {
		clusterIdToPool[clusterInfo.ClusterId] = clusterInfo.Pool
	}

	metricsRecorder := metrics.NewJobMetricsRecorder()
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
			jobResources := job.TotalResourceRequest()
			runTime := now.Sub(runInfo.StartTime)
			priorityClass := getPriorityClass(job)
			metricsRecorder.RecordJobRuntime(pool, priorityClass, runTime)
			metricsRecorder.RecordResources(pool, priorityClass, jobResources.AsFloat())
		}
	}

	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	c.runningJobMetrics[queue.Name] = metricsRecorder.Metrics()
	return nil
}

func (c *QueueCache) updateQueuedNonMatchingJobs(queueName string, nonMatchingClustersById map[string]stringSet) {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	c.queueNonMatchingJobIds[queueName] = nonMatchingClustersById
}

func (c *QueueCache) GetQueuedJobMetrics(queueName string) []*metrics.QueueMetrics {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	return c.queuedJobMetrics[queueName]
}

func (c *QueueCache) GetRunningJobMetrics(queueName string) []*metrics.QueueMetrics {
	c.refreshMutex.Lock()
	defer c.refreshMutex.Unlock()
	return c.runningJobMetrics[queueName]
}

func getPriorityClass(job *api.Job) string {
	podSpec := util.PodSpecFromJob(job)
	if podSpec != nil {
		return podSpec.PriorityClassName
	}
	return ""
}
