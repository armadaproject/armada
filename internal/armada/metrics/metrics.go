package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

const MetricPrefix = "armada_"

type QueueMetricProvider interface {
	GetQueuedJobMetrics(queueName string) []*QueueMetrics
	GetRunningJobMetrics(queueName string) []*QueueMetrics
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
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var minQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_min",
	"Min resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var maxQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_max",
	"Max resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var medianQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_median",
	"Median resource required by queued jobs",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var countQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_count",
	"Count of queued jobs requiring resource",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var minQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_min",
	"Min queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var maxQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_max",
	"Max queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var medianQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_median",
	"Median queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var queueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds",
	"Queued time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var minJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_min",
	"Min run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var maxJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_max",
	"Max run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var medianJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_median",
	"Median run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var jobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds",
	"Run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
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
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var maxQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_max",
	"Max resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var medianQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_median",
	"Median resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
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

	usageReports, e := c.usageRepository.GetClusterUsageReports()
	if e != nil {
		log.Errorf("Error while getting queue usage metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

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
		queuedJobMetrics := c.queueMetrics.GetQueuedJobMetrics(q.Name)
		runningJobMetrics := c.queueMetrics.GetRunningJobMetrics(q.Name)
		for _, m := range queuedJobMetrics {
			queueDurations := m.Durations
			if queueDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(queueDurationDesc, m.Durations.GetCount(),
					queueDurations.GetSum(), queueDurations.GetBuckets(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(minQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMin(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(maxQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMax(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(medianQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMedian(), m.Pool, m.PriorityClass, q.Name)
			}

			for resourceType, amount := range m.Resources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(queueResourcesDesc, prometheus.GaugeValue, amount.GetSum(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(minQueueResourcesDesc, prometheus.GaugeValue, amount.GetMin(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(maxQueueResourcesDesc, prometheus.GaugeValue, amount.GetMax(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(medianQueueResourcesDesc, prometheus.GaugeValue, amount.GetMedian(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(countQueueResourcesDesc, prometheus.GaugeValue, float64(amount.GetCount()), m.Pool, m.PriorityClass, q.Name, resourceType)
				}
			}
		}

		for _, m := range runningJobMetrics {
			runningJobDurations := m.Durations
			if runningJobDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(jobRunDurationDesc, runningJobDurations.GetCount(),
					runningJobDurations.GetSum(), runningJobDurations.GetBuckets(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(minJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMin(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(maxJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMax(), m.Pool, m.PriorityClass, q.Name)
				metrics <- prometheus.MustNewConstMetric(medianJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMedian(), m.Pool, m.PriorityClass, q.Name)
			}

			for resourceType, amount := range m.Resources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(minQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMin(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(maxQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMax(), m.Pool, m.PriorityClass, q.Name, resourceType)
					metrics <- prometheus.MustNewConstMetric(medianQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMedian(), m.Pool, m.PriorityClass, q.Name, resourceType)
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
							armadaresource.QuantityAsFloat64(value),
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
							armadaresource.QuantityAsFloat64(value),
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
						armadaresource.QuantityAsFloat64(value),
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
						armadaresource.QuantityAsFloat64(value),
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
						armadaresource.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						resourceType,
						nodeTypeUsage.NodeType.Id)
				}
				for resourceType, value := range nodeTypeUsage.AvailableCapacity {
					metrics <- prometheus.MustNewConstMetric(
						clusterAvailableCapacity,
						prometheus.GaugeValue,
						armadaresource.QuantityAsFloat64(value),
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
					armadaresource.QuantityAsFloat64(value),
					cluster,
					report.Pool,
					resourceType,
					report.Pool)
			}

			for resourceType, value := range report.ClusterAvailableCapacity {
				metrics <- prometheus.MustNewConstMetric(
					clusterAvailableCapacity,
					prometheus.GaugeValue,
					armadaresource.QuantityAsFloat64(value),
					cluster,
					report.Pool,
					resourceType,
					report.Pool)
			}
		}
	}
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
