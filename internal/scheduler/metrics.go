package scheduler

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/queue"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// Metrics Recorders associated with a queue
type queueState struct {
	queuedJobRecorder  *commonmetrics.JobMetricsRecorder
	runningJobRecorder *commonmetrics.JobMetricsRecorder
	queue              *api.Queue
}

// metricProvider is a simple implementation of QueueMetricProvider
type metricProvider struct {
	queueStates map[string]*queueState
}

func (m metricProvider) GetAllQueues() []*api.Queue {
	return slices.Map(maps.Values(m.queueStates), func(state *queueState) *api.Queue {
		return state.queue
	})
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
	jobDb                 *jobdb.JobDb
	queueCache            queue.QueueCache
	executorRepository    database.ExecutorRepository
	pools                 []configuration.PoolConfig
	refreshPeriod         time.Duration
	clock                 clock.WithTicker
	state                 atomic.Value
	floatingResourceTypes *floatingresources.FloatingResourceTypes
}

func NewMetricsCollector(
	jobDb *jobdb.JobDb,
	queueCache queue.QueueCache,
	executorRepository database.ExecutorRepository,
	pools []configuration.PoolConfig,
	refreshPeriod time.Duration,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
) *MetricsCollector {
	return &MetricsCollector{
		jobDb:                 jobDb,
		queueCache:            queueCache,
		executorRepository:    executorRepository,
		pools:                 pools,
		refreshPeriod:         refreshPeriod,
		clock:                 clock.RealClock{},
		state:                 atomic.Value{},
		floatingResourceTypes: floatingResourceTypes,
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
				ctx.Logger().
					WithStacktrace(err).
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
	queues, err := c.queueCache.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	provider := metricProvider{queueStates: make(map[string]*queueState, len(queues))}
	queuedJobsCount := make(map[string]int, len(queues))
	schedulingKeysByQueue := make(map[string]map[internaltypes.SchedulingKey]bool, len(queues))

	for _, queue := range queues {
		provider.queueStates[queue.Name] = &queueState{
			queuedJobRecorder:  commonmetrics.NewJobMetricsRecorder(),
			runningJobRecorder: commonmetrics.NewJobMetricsRecorder(),
			queue:              queue,
		}
		queuedJobsCount[queue.Name] = 0
		schedulingKeysByQueue[queue.Name] = map[internaltypes.SchedulingKey]bool{}
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

		pools := job.ResolvedPools()

		priorityClass := job.JobSchedulingInfo().PriorityClassName
		resourceRequirements := job.JobSchedulingInfo().PodRequirements.ResourceRequirements.Requests
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
			queuedTime := time.Unix(0, job.Created())
			if job.HasRuns() {
				terminationTimeOfLatestRun := job.LatestRun().TerminatedTime()
				if terminationTimeOfLatestRun != nil && terminationTimeOfLatestRun.After(queuedTime) {
					queuedTime = *terminationTimeOfLatestRun
				}
			}
			timeInState = currentTime.Sub(queuedTime)
			queuedJobsCount[job.Queue()]++
			schedulingKeysByQueue[job.Queue()][job.SchedulingKey()] = true
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
		for _, pool := range pools {
			recorder.RecordJobRuntime(pool, priorityClass, timeInState)
			recorder.RecordResources(pool, priorityClass, jobResources)
		}
	}

	queuedDistinctSchedulingKeysCount := armadamaps.MapValues(schedulingKeysByQueue, func(schedulingKeys map[internaltypes.SchedulingKey]bool) int {
		return len(schedulingKeys)
	})

	queueMetrics := commonmetrics.CollectQueueMetrics(queuedJobsCount, queuedDistinctSchedulingKeysCount, provider)
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

type clusterCordonedStatus struct {
	status    float64
	reason    string
	setByUser string
}

func (c *MetricsCollector) updateClusterMetrics(ctx *armadacontext.Context) ([]prometheus.Metric, error) {
	executors, err := c.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}
	executorSettings, err := c.executorRepository.GetExecutorSettings(ctx)
	if err != nil {
		return nil, err
	}

	cordonedStatusByCluster := map[string]*clusterCordonedStatus{}
	phaseCountByQueue := map[queuePhaseMetricKey]int{}
	allocatedResourceByQueue := map[queueMetricKey]resource.ComputeResources{}
	usedResourceByQueue := map[queueMetricKey]resource.ComputeResources{}
	availableResourceByCluster := map[clusterMetricKey]resource.ComputeResources{}
	totalResourceByCluster := map[clusterMetricKey]resource.ComputeResources{}
	totalFarmResourceByCluster := map[clusterMetricKey]resource.ComputeResources{}
	schedulableNodeCountByCluster := map[clusterMetricKey]int{}
	totalNodeCountByCluster := map[clusterMetricKey]int{}

	poolToAwayPools := map[string][]string{}
	for _, poolConfig := range c.pools {
		for _, ap := range poolConfig.AwayPools {
			poolToAwayPools[ap] = append(poolToAwayPools[ap], poolConfig.Name)
		}
	}

	txn := c.jobDb.ReadTxn()

	for _, executorSetting := range executorSettings {
		if executorSetting.Cordoned {
			// We may have settings for executors that don't exist in the repository.
			cordonedStatusByCluster[executorSetting.ExecutorId] = &clusterCordonedStatus{
				status:    1.0,
				reason:    executorSetting.CordonReason,
				setByUser: executorSetting.SetByUser,
			}
		} else {
			cordonedStatusByCluster[executorSetting.ExecutorId] = &clusterCordonedStatus{
				status:    0.0,
				reason:    executorSetting.CordonReason,
				setByUser: executorSetting.SetByUser,
			}
		}
	}

	for _, executor := range executors {
		if _, statusExists := cordonedStatusByCluster[executor.Id]; !statusExists {
			// We may not have executorSettings for all known executors, but we still want a cordon status metric for them.
			cordonedStatusByCluster[executor.Id] = &clusterCordonedStatus{
				status:    0.0,
				reason:    "",
				setByUser: "",
			}
		}
		for _, node := range executor.Nodes {
			nodePool := node.GetPool()
			awayPools := poolToAwayPools[nodePool]

			clusterKey := clusterMetricKey{
				cluster:  executor.Id,
				pool:     nodePool,
				nodeType: node.ReportingNodeType,
			}

			if _, ok := schedulableNodeCountByCluster[clusterKey]; !ok {
				schedulableNodeCountByCluster[clusterKey] = 0
			}

			awayClusterKeys := make([]clusterMetricKey, 0, len(awayPools))
			for _, ap := range awayPools {
				awayClusterKeys = append(awayClusterKeys, clusterMetricKey{
					cluster:  executor.Id,
					pool:     ap,
					nodeType: node.ReportingNodeType,
				})
			}

			uncordonedNodeResources := node.AvailableArmadaResource()
			nodeResources := node.AvailableArmadaResource()

			if !node.Unschedulable && cordonedStatusByCluster[executor.Id].status != 1.0 {
				schedulableNodeCountByCluster[clusterKey]++
			} else {
				// We still want to publish these metrics, just with zeroed values
				uncordonedNodeResources.Zero()
			}

			addToResourceListMap(availableResourceByCluster, clusterKey, uncordonedNodeResources)
			addToResourceListMap(totalFarmResourceByCluster, clusterKey, nodeResources)

			// Add available resources to the away cluster pool
			for _, awayClusterKey := range awayClusterKeys {
				addToResourceListMap(availableResourceByCluster, awayClusterKey, uncordonedNodeResources)
				addToResourceListMap(totalFarmResourceByCluster, awayClusterKey, nodeResources)
			}
			totalNodeCountByCluster[clusterKey]++

			// Add total resources to the home cluster pool
			addToResourceListMap(totalResourceByCluster, clusterKey, node.TotalResources.ToComputeResources())

			// Add total resources to the away cluster pool
			for _, awayClusterKey := range awayClusterKeys {
				addToResourceListMap(totalResourceByCluster, awayClusterKey, node.TotalResources.ToComputeResources())
			}

			for _, resourceUsageQp := range node.ResourceUsageByQueueAndPool {
				queueKey := queueMetricKey{
					cluster:   executor.Id,
					pool:      resourceUsageQp.Pool,
					queueName: resourceUsageQp.Queue,
					nodeType:  node.ReportingNodeType,
				}
				addToResourceListMap(usedResourceByQueue, queueKey, resourceUsageQp.Resources.ToComputeResources())
			}

			for runId, jobRunState := range node.StateByJobRunId {
				job := txn.GetByRunId(runId)
				if job != nil && job.LatestRun() != nil {
					jobPool := job.LatestRun().Pool()
					phase := schedulerobjects.JobRunState_name[int32(jobRunState)]
					key := queuePhaseMetricKey{
						cluster:   executor.Id,
						pool:      jobPool,
						queueName: job.Queue(),
						nodeType:  node.ReportingNodeType,
						// Convert to string with first letter capitalised
						phase: strings.Title(strings.ToLower(phase)),
					}
					phaseCountByQueue[key]++

					podRequirements := job.PodRequirements()
					if podRequirements != nil {
						jobRequirements := resource.FromResourceList(podRequirements.ResourceRequirements.Requests)
						queueKey := queueMetricKey{
							cluster:       executor.Id,
							pool:          jobPool,
							queueName:     job.Queue(),
							priorityClass: job.PriorityClassName(),
							nodeType:      node.ReportingNodeType,
						}
						addToResourceListMap(allocatedResourceByQueue, queueKey, jobRequirements.ToComputeResources())

						// If the job is running on its home pool, then remove the resources from all the away pools
						if jobPool == nodePool {
							for _, awayClusterKey := range awayClusterKeys {
								subtractFromResourceListMap(totalResourceByCluster, awayClusterKey, jobRequirements.ToComputeResources())
								subtractFromResourceListMap(availableResourceByCluster, awayClusterKey, jobRequirements.ToComputeResources())
							}
						}
					}
				}
			}
		}
	}

	for _, pool := range c.floatingResourceTypes.AllPools() {
		totalFloatingResources := c.floatingResourceTypes.GetTotalAvailableForPoolAsMap(pool)
		clusterKey := clusterMetricKey{
			cluster:  "floating",
			pool:     pool,
			nodeType: "",
		}
		addToResourceListMap(availableResourceByCluster, clusterKey, totalFloatingResources)
		addToResourceListMap(totalResourceByCluster, clusterKey, totalFloatingResources)
	}

	clusterMetrics := make([]prometheus.Metric, 0, len(phaseCountByQueue))
	for k, v := range phaseCountByQueue {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueLeasedPodCount(float64(v), k.cluster, k.pool, k.queueName, k.phase, k.nodeType))
	}
	for k, r := range allocatedResourceByQueue {
		for resourceKey, resourceValue := range r {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueAllocated(resource.QuantityAsFloat64(resourceValue), k.queueName, k.cluster, k.pool, k.priorityClass, resourceKey, k.nodeType))
		}
	}
	for k, r := range usedResourceByQueue {
		for resourceKey, resourceValue := range r {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewQueueUsed(resource.QuantityAsFloat64(resourceValue), k.queueName, k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, r := range availableResourceByCluster {
		for resourceKey, resourceValue := range r {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterAvailableCapacity(resource.QuantityAsFloat64(resourceValue), k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, r := range totalFarmResourceByCluster {
		for resourceKey, resourceValue := range r {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterFarmCapacity(resource.QuantityAsFloat64(resourceValue), k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, r := range totalResourceByCluster {
		for resourceKey, resourceValue := range r {
			clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterTotalCapacity(resource.QuantityAsFloat64(resourceValue), k.cluster, k.pool, resourceKey, k.nodeType))
		}
	}
	for k, v := range schedulableNodeCountByCluster {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterAvailableCapacity(float64(v), k.cluster, k.pool, "nodes", k.nodeType))
	}
	for k, v := range totalNodeCountByCluster {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterTotalCapacity(float64(v), k.cluster, k.pool, "nodes", k.nodeType))
	}
	for cluster, v := range cordonedStatusByCluster {
		clusterMetrics = append(clusterMetrics, commonmetrics.NewClusterCordonedStatus(v.status, cluster, v.reason, v.setByUser))
	}
	return clusterMetrics, nil
}

func addToResourceListMap[K comparable](m map[K]resource.ComputeResources, key K, value resource.ComputeResources) {
	if _, exists := m[key]; !exists {
		m[key] = resource.ComputeResources{}
	}
	newValue := m[key]
	newValue.Add(value)
	m[key] = newValue
}

func subtractFromResourceListMap[K comparable](m map[K]resource.ComputeResources, key K, value resource.ComputeResources) {
	if _, exists := m[key]; !exists {
		m[key] = resource.ComputeResources{}
	}
	newValue := m[key]
	newValue.Sub(value)
	m[key] = newValue
}
