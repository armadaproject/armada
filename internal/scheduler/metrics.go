package scheduler

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// Metrics Recorders associated with a queue
type queueState struct {
	queuedJobRecorder  *commonmetrics.JobMetricsRecorder
	runningJobRecorder *commonmetrics.JobMetricsRecorder
}

// metricProvider is a simple implementation of QueueMetricProvider
type metricProvider struct {
	queueStates map[string]*queueState
}

func (m metricProvider) GetQueuedJobMetrics(queueName string) []*commonmetrics.QueueMetrics {
	state, ok := m.queueStates[queueName]
	if ok {
		return state.queuedJobRecorder.Metrics()
	}
	return nil
}

func (m metricProvider) GetRunningJobMetrics(queueName string) []*commonmetrics.QueueMetrics {
	state, ok := m.queueStates[queueName]
	if ok {
		return state.runningJobRecorder.Metrics()
	}
	return nil
}

// MetricsCollector is a Prometheus Collector that handles scheduler metrics.
// The metrics themselves are calculated asynchronously every refreshPeriod
type MetricsCollector struct {
	jobDb              *jobdb.JobDb
	queueRepository    repository.QueueRepository
	executorRepository database.ExecutorRepository
	poolAssigner       PoolAssigner
	refreshPeriod      time.Duration
	clock              clock.Clock
	state              atomic.Value
}

func NewMetricsCollector(
	jobDb *jobdb.JobDb,
	queueRepository repository.QueueRepository,
	executorRepository database.ExecutorRepository,
	poolAssigner PoolAssigner,
	refreshPeriod time.Duration,
) *MetricsCollector {
	return &MetricsCollector{
		jobDb:              jobDb,
		queueRepository:    queueRepository,
		executorRepository: executorRepository,
		poolAssigner:       poolAssigner,
		refreshPeriod:      refreshPeriod,
		clock:              clock.RealClock{},
		state:              atomic.Value{},
	}
}

// Run enters s a loop which updates the metrics every refreshPeriod until the supplied context is cancelled
func (c *MetricsCollector) Run(ctx *armadacontext.Context) error {
	ticker := c.clock.NewTicker(c.refreshPeriod)
	ctx.Infof("Will update metrics every %s", c.refreshPeriod)
	for {
		select {
		case <-ctx.Done():
			ctx.Debugf("Context cancelled, returning..")
			return nil
		case <-ticker.C():
			err := c.refresh(ctx)
			if err != nil {
				logging.
					WithStacktrace(ctx, err).
					Warnf("error refreshing metrics state")
			}
		}
	}
}

// Describe returns all descriptions of the collector.
func (c *MetricsCollector) Describe(out chan<- *prometheus.Desc) {
	commonmetrics.Describe(out)
}

// Collect returns the current state of all metrics of the collector.
func (c *MetricsCollector) Collect(metrics chan<- prometheus.Metric) {
	state, ok := c.state.Load().([]prometheus.Metric)
	if ok {
		for _, m := range state {
			metrics <- m
		}
	}
}

func (c *MetricsCollector) refresh(ctx *armadacontext.Context) error {
	ctx.Debugf("Refreshing prometheus metrics")
	start := time.Now()
	queueMetrics, err := c.updateQueueMetrics(ctx)
	if err != nil {
		return err
	}
	clusterMetrics, err := c.updateClusterMetrics(ctx)
	if err != nil {
		return err
	}
	allMetrics := append(queueMetrics, clusterMetrics...)
	c.state.Store(allMetrics)
	ctx.Debugf("Refreshed prometheus metrics in %s", time.Since(start))
	return nil
}

func (c *MetricsCollector) updateQueueMetrics(ctx *armadacontext.Context) ([]prometheus.Metric, error) {
	queues, err := c.queueRepository.GetAllQueues(ctx)
	if err != nil {
		return nil, err
	}

	provider := metricProvider{queueStates: make(map[string]*queueState, len(queues))}
	queuedJobsCount := make(map[string]int, len(queues))
	for _, queue := range queues {
		provider.queueStates[queue.Name] = &queueState{
			queuedJobRecorder:  commonmetrics.NewJobMetricsRecorder(),
			runningJobRecorder: commonmetrics.NewJobMetricsRecorder(),
		}
		queuedJobsCount[queue.Name] = 0
	}

	err = c.poolAssigner.Refresh(ctx)
	if err != nil {
		return nil, err
	}

	currentTime := c.clock.Now()
	for _, job := range c.jobDb.ReadTxn().GetAll() {
		// Don't calculate metrics for dead jobs
		if job.InTerminalState() {
			continue
		}
		qs, ok := provider.queueStates[job.Queue()]
		if !ok {
			ctx.Warnf("job %s is in queue %s, but this queue does not exist; skipping", job.Id(), job.Queue())
			continue
		}

		pool, err := c.poolAssigner.AssignPool(job)
		if err != nil {
			return nil, err
		}

		priorityClass := job.JobSchedulingInfo().PriorityClassName
		resourceRequirements := job.JobSchedulingInfo().GetObjectRequirements()[0].GetPodRequirements().GetResourceRequirements().Requests
		jobResources := make(map[string]float64)
		for key, value := range resourceRequirements {
			jobResources[string(key)] = resource.QuantityAsFloat64(value)
		}

		var recorder *commonmetrics.JobMetricsRecorder
		var timeInState time.Duration
		if job.InTerminalState() {
			// Jobs in a terminal state should have been removed from the jobDb.
			continue
		} else if job.Queued() {
			if run := job.LatestRun(); run != nil && !run.InTerminalState() {
				ctx.Warnf("job %s is marked as queued but has active runs: %s", job.Id(), job)
				continue
			}
			recorder = qs.queuedJobRecorder
			timeInState = currentTime.Sub(time.Unix(0, job.Created()))
			queuedJobsCount[job.Queue()]++
		} else {
			run := job.LatestRun()
			if run == nil {
				ctx.Warnf("job %s is active and not marked as queued, but has no runs associated with it: %s", job.Id(), job)
				continue
			} else if run.InTerminalState() {
				// TODO(albin): Jobs are not always updated in the same transaction as runs,
				//              so jobs will briefly be marked as inactive despite having a terminal run associated with it.
				// ctx.Warnf("job %s is active and not marked as queued, but its most recent run is in a terminal state: %s", job.Id(), job)
				continue
			}
			recorder = qs.runningJobRecorder
			timeInState = currentTime.Sub(time.Unix(0, run.Created()))
		}
		recorder.RecordJobRuntime(pool, priorityClass, timeInState)
		recorder.RecordResources(pool, priorityClass, jobResources)
	}

	queueMetrics := commonmetrics.CollectQueueMetrics(queuedJobsCount, provider)
	return queueMetrics, nil
}

type queueMetricKey struct {
	cluster       string
	pool          string
	queueName     string
	nodeType      string
	priorityClass string
}

type queuePhaseMetricKey struct {
	cluster   string
	pool      string
	queueName string
	nodeType  string
	phase     string
}

type clusterMetricKey struct {
	cluster  string
	pool     string
	nodeType string
}

func (c *MetricsCollector) updateClusterMetrics(ctx *armadacontext.Context) ([]prometheus.Metric, error) {
	executors, err := c.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}

	phaseCountByQueue := map[queuePhaseMetricKey]int{}
	allocatedResourceByQueue := map[queueMetricKey]schedulerobjects.ResourceList{}
	usedResourceByQueue := map[queueMetricKey]schedulerobjects.ResourceList{}
	availableResourceByCluster := map[clusterMetricKey]schedulerobjects.ResourceList{}
	totalResourceByCluster := map[clusterMetricKey]schedulerobjects.ResourceList{}
	schedulableNodeCountByCluster := map[clusterMetricKey]int{}
	totalNodeCountByCluster := map[clusterMetricKey]int{}

	txn := c.jobDb.ReadTxn()
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			clusterKey := clusterMetricKey{
				cluster:  executor.Id,
				pool:     executor.Pool,
				nodeType: node.ReportingNodeType,
			}
			if !node.Unschedulable {
				addToResourceListMap(availableResourceByCluster, clusterKey, node.AvailableArmadaResource())
				schedulableNodeCountByCluster[clusterKey]++
			}
			addToResourceListMap(totalResourceByCluster, clusterKey, node.TotalResources)
			totalNodeCountByCluster[clusterKey]++

			for queueName, resourceUsage := range node.ResourceUsageByQueue {
				queueKey := queueMetricKey{
					cluster:   executor.Id,
					pool:      executor.Pool,
					queueName: queueName,
					nodeType:  node.ReportingNodeType,
				}
				addToResourceListMap(usedResourceByQueue, queueKey, *resourceUsage)
			}

			for runId, jobRunState := range node.StateByJobRunId {
				job := txn.GetByRunId(uuid.MustParse(runId))
				if job != nil {
					phase := schedulerobjects.JobRunState_name[int32(jobRunState)]
					key := queuePhaseMetricKey{
						cluster:   executor.Id,
						pool:      executor.Pool,
						queueName: job.Queue(),
						nodeType:  node.ReportingNodeType,
						// Convert to string with first letter capitalised
						phase: strings.Title(strings.ToLower(phase)),
					}
					phaseCountByQueue[key]++

					podRequirements := job.PodRequirements()
					if podRequirements != nil {
						queueKey := queueMetricKey{
							cluster:       executor.Id,
							pool:          executor.Pool,
							queueName:     job.Queue(),
							priorityClass: job.GetPriorityClassName(),
							nodeType:      node.ReportingNodeType,
						}
						addToResourceListMap(allocatedResourceByQueue, queueKey, schedulerobjects.ResourceListFromV1ResourceList(podRequirements.ResourceRequirements.Requests))
					}
				}
			}
		}
	}

	clusterMetrics := make([]prometheus.Metric, 0, len(phaseCountByQueue))
	for k, v := range phaseCountByQueue {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueLeasedPodCount(float64(v), k.cluster, k.pool, k.queueName, k.phase, k.nodeType))
	}
	for k, r := range allocatedResourceByQueue {
		for resourceKey, resourceValue := range r.Resources {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueAllocated(resource.QuantityAsFloat64(resourceValue), k.queueName, k.cluster, k.pool, k.priorityClass, resourceKey, k.nodeType))
		}
	}
	for k, r := range usedResourceByQueue {
		for resourceKey, resourceValue := range r.Resources {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueUsed(resource.QuantityAsFloat64(resourceValue), k.queueName, k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, r := range availableResourceByCluster {
		for resourceKey, resourceValue := range r.Resources {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterAvailableCapacity(resource.QuantityAsFloat64(resourceValue), k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, r := range totalResourceByCluster {
		for resourceKey, resourceValue := range r.Resources {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterTotalCapacity(resource.QuantityAsFloat64(resourceValue), k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, v := range schedulableNodeCountByCluster {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterAvailableCapacity(float64(v), k.cluster, k.pool, "nodes", k.nodeType))
	}
	for k, v := range totalNodeCountByCluster {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterTotalCapacity(float64(v), k.cluster, k.pool, "nodes", k.nodeType))
	}
	return clusterMetrics, nil
}

func addToResourceListMap[K comparable](m map[K]schedulerobjects.ResourceList, key K, value schedulerobjects.ResourceList) {
	if _, exists := m[key]; !exists {
		m[key] = schedulerobjects.ResourceList{}
	}
	newValue := m[key]
	newValue.Add(value)
	m[key] = newValue
}
