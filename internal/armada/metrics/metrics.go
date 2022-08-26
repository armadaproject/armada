package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

const MetricPrefix = "armada_"

type QueueMetricProvider interface {
	GetQueueMetrics(queueName string) *QueueMetrics
}

func ExposeDataMetrics(
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	usageRepository repository.UsageRepository,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	queueMetrics QueueMetricProvider,
) *QueueInfoCollector {
	collector := &QueueInfoCollector{
		queueRepository:          queueRepository,
		jobRepository:            jobRepository,
		usageRepository:          usageRepository,
		schedulingInfoRepository: schedulingInfoRepository,
		queueMetrics:             queueMetrics,
	}
	prometheus.MustRegister(collector)
	return collector
}

type QueueInfoCollector struct {
	queueRepository          repository.QueueRepository
	jobRepository            repository.JobRepository
	usageRepository          repository.UsageRepository
	schedulingInfoRepository repository.SchedulingInfoRepository
	queueMetrics             QueueMetricProvider
}

var queueSizeDesc = prometheus.NewDesc(
	MetricPrefix+"queue_size",
	"Number of jobs in a queue",
	[]string{"queueName"},
	nil,
)

var queuePriorityDesc = prometheus.NewDesc(
	MetricPrefix+"queue_priority",
	"Priority of a queue",
	[]string{"pool", "queueName"},
	nil,
)

var queueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued",
	"Resource required by queued jobs",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var minQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_min",
	"Min resource required by queued job",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var maxQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_max",
	"Max resource required by queued job",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var medianQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_median",
	"Median resource required by queued jobs",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var countQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_count",
	"Count of queued jobs requiring resource",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var minQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_min",
	"Min queue time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var maxQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_max",
	"Max queue time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var medianQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_median",
	"Median queue time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var queueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds",
	"Queued time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var minJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_min",
	"Min run time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var maxJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_max",
	"Max run time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var medianJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_median",
	"Median run time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var jobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds",
	"Run time for Armada jobs",
	[]string{"pool", "queueName"},
	nil,
)

var queueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated",
	"Resource allocated to running jobs of a queue",
	[]string{"cluster", "pool", "queueName", "resourceType", "nodeType"},
	nil,
)

var minQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_min",
	"Min resource allocated by a running job",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var maxQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_max",
	"Max resource allocated by a running job",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var medianQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_median",
	"Median resource allocated by a running job",
	[]string{"pool", "queueName", "resourceType"},
	nil,
)

var queueUsedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_used",
	"Resource actually being used by running jobs of a queue",
	[]string{"cluster", "pool", "queueName", "resourceType", "nodeType"},
	nil,
)

var queueLeasedPodCountDesc = prometheus.NewDesc(
	MetricPrefix+"queue_leased_pod_count",
	"Number of leased pods",
	[]string{"cluster", "pool", "queueName", "phase", "nodeType"},
	nil,
)

var clusterCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_capacity",
	"Cluster capacity",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var clusterAvailableCapacity = prometheus.NewDesc(
	MetricPrefix+"cluster_available_capacity",
	"Cluster capacity available for Armada jobs",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- queueSizeDesc
	desc <- queuePriorityDesc
	desc <- queueDurationDesc
	desc <- minQueueDurationDesc
	desc <- maxQueueDurationDesc
	desc <- medianQueueDurationDesc
	desc <- minQueueResourcesDesc
	desc <- maxQueueResourcesDesc
	desc <- medianQueueResourcesDesc
	desc <- jobRunDurationDesc
	desc <- minJobRunDurationDesc
	desc <- maxJobRunDurationDesc
	desc <- medianJobRunDurationDesc
	desc <- minQueueAllocatedDesc
	desc <- maxQueueAllocatedDesc
	desc <- medianQueueAllocatedDesc
}

func (c *QueueInfoCollector) Collect(metrics chan<- prometheus.Metric) {
	queues, e := c.queueRepository.GetAllQueues()
	if e != nil {
		log.Errorf("Error while getting queue metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	queueSizes, e := c.jobRepository.GetQueueSizes(queue.QueuesToAPI(queues))
	if e != nil {
		log.Errorf("Error while getting queue size metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	clusterSchedulingInfo, e := c.schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		log.Errorf("Error while getting cluster reports %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	usageReports, e := c.usageRepository.GetClusterUsageReports()
	if e != nil {
		log.Errorf("Error while getting queue usage metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	activeClusterInfo := scheduling.FilterActiveClusterSchedulingInfoReports(clusterSchedulingInfo)
	runDurationsByPool, runResourceByPool := c.calculateRunningJobStats(queue.QueuesToAPI(queues), activeClusterInfo)

	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	clusterPriorities, e := c.usageRepository.GetClusterPriorities(scheduling.GetClusterReportIds(activeClusterReports))
	if e != nil {
		log.Errorf("Error while getting queue priority metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	clustersByPool := scheduling.GroupByPool(activeClusterReports)
	for pool, poolReports := range clustersByPool {
		poolPriorities := map[string]map[string]float64{}
		for cluster := range poolReports {
			poolPriorities[cluster] = clusterPriorities[cluster]
		}
		queuePriority := scheduling.CalculateQueuesPriorityInfo(poolPriorities, poolReports, queue.QueuesToAPI(queues))
		for queue, priority := range queuePriority {
			metrics <- prometheus.MustNewConstMetric(queuePriorityDesc, prometheus.GaugeValue, priority.Priority, pool, queue.Name)
		}
	}

	for i, q := range queues {
		metrics <- prometheus.MustNewConstMetric(queueSizeDesc, prometheus.GaugeValue, float64(queueSizes[i]), q.Name)
		queueMetrics := c.queueMetrics.GetQueueMetrics(q.Name)
		for pool, queueDurations := range queueMetrics.Durations {
			if queueDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(queueDurationDesc, queueDurations.GetCount(),
					queueDurations.GetSum(), queueDurations.GetBuckets(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(minQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMin(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(maxQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMax(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(medianQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMedian(), pool, q.Name)
			}
		}

		for pool, runningJobDurations := range runDurationsByPool[q.Name] {
			if runningJobDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(jobRunDurationDesc, runningJobDurations.GetCount(),
					runningJobDurations.GetSum(), runningJobDurations.GetBuckets(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(minJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMin(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(maxJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMax(), pool, q.Name)
				metrics <- prometheus.MustNewConstMetric(medianJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMedian(), pool, q.Name)
			}
		}

		for pool, poolResources := range queueMetrics.Resources {
			for resourceType, amount := range poolResources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(queueResourcesDesc, prometheus.GaugeValue, amount.GetSum(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(minQueueResourcesDesc, prometheus.GaugeValue, amount.GetMin(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(maxQueueResourcesDesc, prometheus.GaugeValue, amount.GetMax(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(medianQueueResourcesDesc, prometheus.GaugeValue, amount.GetMedian(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(countQueueResourcesDesc, prometheus.GaugeValue, float64(amount.GetCount()), pool, q.Name, resourceType)
				}
			}
		}

		for pool, poolRunningResources := range runResourceByPool[q.Name] {
			for resourceType, amount := range poolRunningResources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(minQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMin(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(maxQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMax(), pool, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(medianQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMedian(), pool, q.Name, resourceType)
				}
			}
		}
	}

	c.recordQueueUsageMetrics(metrics, activeClusterReports)
	c.recordClusterCapacityMetrics(metrics, activeClusterReports)
}

func (c *QueueInfoCollector) recordQueueUsageMetrics(metrics chan<- prometheus.Metric, activeClusterUsageReports map[string]*api.ClusterUsageReport) {
	for cluster, report := range activeClusterUsageReports {
		if len(report.NodeTypeUsageReports) > 0 {
			for _, nodeTypeUsage := range report.NodeTypeUsageReports {
				for _, queueReport := range nodeTypeUsage.Queues {
					for resourceType, value := range queueReport.Resources {
						metrics <- prometheus.MustNewConstMetric(
							queueAllocatedDesc,
							prometheus.GaugeValue,
							common.QuantityAsFloat64(value),
							cluster,
							report.Pool,
							queueReport.Name,
							resourceType,
							nodeTypeUsage.NodeType.Id)
					}
					for resourceType, value := range queueReport.ResourcesUsed {
						metrics <- prometheus.MustNewConstMetric(
							queueUsedDesc,
							prometheus.GaugeValue,
							common.QuantityAsFloat64(value),
							cluster,
							report.Pool,
							queueReport.Name,
							resourceType,
							nodeTypeUsage.NodeType.Id)
					}
					for phase, count := range queueReport.CountOfPodsByPhase {
						metrics <- prometheus.MustNewConstMetric(
							queueLeasedPodCountDesc,
							prometheus.GaugeValue,
							float64(count),
							cluster,
							report.Pool,
							queueReport.Name,
							phase,
							nodeTypeUsage.NodeType.Id)
					}
				}
			}
		} else if len(report.Queues) > 0 {
			for _, queueReport := range report.Queues {
				for resourceType, value := range queueReport.Resources {
					metrics <- prometheus.MustNewConstMetric(
						queueAllocatedDesc,
						prometheus.GaugeValue,
						common.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						queueReport.Name,
						resourceType,
						report.Pool)
				}
				for resourceType, value := range queueReport.ResourcesUsed {
					metrics <- prometheus.MustNewConstMetric(
						queueUsedDesc,
						prometheus.GaugeValue,
						common.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						queueReport.Name,
						resourceType,
						report.Pool)
				}
			}
		}
	}
}

func (c *QueueInfoCollector) recordClusterCapacityMetrics(metrics chan<- prometheus.Metric, activeClusterUsageReports map[string]*api.ClusterUsageReport) {
	for cluster, report := range activeClusterUsageReports {
		if len(report.NodeTypeUsageReports) > 0 {
			for _, nodeTypeUsage := range report.NodeTypeUsageReports {
				for resourceType, value := range nodeTypeUsage.Capacity {
					metrics <- prometheus.MustNewConstMetric(
						clusterCapacityDesc,
						prometheus.GaugeValue,
						common.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						resourceType,
						nodeTypeUsage.NodeType.Id)
				}
				for resourceType, value := range nodeTypeUsage.AvailableCapacity {
					metrics <- prometheus.MustNewConstMetric(
						clusterAvailableCapacity,
						prometheus.GaugeValue,
						common.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						resourceType,
						nodeTypeUsage.NodeType.Id)
				}

				// Add metrics for the number of nodes and the number of nodes available to accept jobs
				metrics <- prometheus.MustNewConstMetric(
					clusterCapacityDesc,
					prometheus.GaugeValue,
					float64(nodeTypeUsage.TotalNodes),
					cluster,
					report.Pool,
					"nodes",
					nodeTypeUsage.NodeType.Id)

				metrics <- prometheus.MustNewConstMetric(
					clusterAvailableCapacity,
					prometheus.GaugeValue,
					float64(nodeTypeUsage.SchedulableNodes),
					cluster,
					report.Pool,
					"nodes",
					nodeTypeUsage.NodeType.Id)
			}
		} else {
			for resourceType, value := range report.ClusterCapacity {
				metrics <- prometheus.MustNewConstMetric(
					clusterCapacityDesc,
					prometheus.GaugeValue,
					common.QuantityAsFloat64(value),
					cluster,
					report.Pool,
					resourceType,
					report.Pool)
			}

			for resourceType, value := range report.ClusterAvailableCapacity {
				metrics <- prometheus.MustNewConstMetric(
					clusterAvailableCapacity,
					prometheus.GaugeValue,
					common.QuantityAsFloat64(value),
					cluster,
					report.Pool,
					resourceType,
					report.Pool)
			}
		}
	}
}

func (c *QueueInfoCollector) calculateRunningJobStats(
	queues []*api.Queue, activeClusterInfos map[string]*api.ClusterSchedulingInfoReport) (
	map[string]map[string]*FloatMetrics, map[string]map[string]ResourceMetrics,
) {
	runDurationMetrics := make(map[string]map[string]*FloatMetrics)
	runResourceMetrics := make(map[string]map[string]ResourceMetrics)

	clusterIdToPool := map[string]string{}
	for _, clusterInfo := range activeClusterInfos {
		clusterIdToPool[clusterInfo.ClusterId] = clusterInfo.Pool
	}

	for _, queue := range queues {
		durationMetricsRecorderByPool := make(map[string]*FloatMetricsRecorder)
		resourceMetricsRecorderByPool := make(map[string]*ResourceMetricsRecorder)
		leasedJobsIds, e := c.jobRepository.GetLeasedJobIds(queue.Name)
		if e != nil {
			log.Errorf("Error getting queue(%s) run duration metrics %s", queue.Name, e)
			continue
		}

		leasedJobs, e := c.jobRepository.GetExistingJobsByIds(leasedJobsIds)
		if e != nil {
			log.Errorf("Error getting queue(%s) run duration metrics %s", queue.Name, e)
			continue
		}

		runInfo, e := c.jobRepository.GetJobRunInfos(leasedJobsIds)
		if e != nil {
			log.Errorf("Error getting queue(%s) run duration metrics %s", queue.Name, e)
			continue
		}

		now := time.Now()
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
				r = NewDefaultJobDurationMetricsRecorder()
				durationMetricsRecorderByPool[pool] = r
			}
			r.Record(runTime.Seconds())

			resource, exists := resourceMetricsRecorderByPool[pool]
			if !exists {
				resource = NewResourceMetricsRecorder()
				resourceMetricsRecorderByPool[pool] = resource
			}
			resource.Record(jobResources.AsFloat())
		}

		if len(durationMetricsRecorderByPool) > 0 {
			runDurationMetrics[queue.Name] = make(map[string]*FloatMetrics, len(durationMetricsRecorderByPool))
			for pool, durations := range durationMetricsRecorderByPool {
				runDurationMetrics[queue.Name][pool] = durations.GetMetrics()
			}
		}

		if len(resourceMetricsRecorderByPool) > 0 {
			runResourceMetrics[queue.Name] = make(map[string]ResourceMetrics, len(resourceMetricsRecorderByPool))
			for pool, durations := range resourceMetricsRecorderByPool {
				runResourceMetrics[queue.Name][pool] = durations.GetMetrics()
			}
		}
	}
	return runDurationMetrics, runResourceMetrics
}

func recordInvalidMetrics(metrics chan<- prometheus.Metric, e error) {
	metrics <- prometheus.NewInvalidMetric(queueSizeDesc, e)
	metrics <- prometheus.NewInvalidMetric(queuePriorityDesc, e)
	metrics <- prometheus.NewInvalidMetric(queueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(queueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(queueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(minQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(maxQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(medianQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(minQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(maxQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(medianQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(jobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(minJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(maxJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(medianJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(minQueueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(maxQueueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(medianQueueAllocatedDesc, e)
}
