package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	commmonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func ExposeDataMetrics(
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	usageRepository repository.UsageRepository,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	queueMetrics commmonmetrics.QueueMetricProvider,
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
	queueMetrics             commmonmetrics.QueueMetricProvider
}

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	commmonmetrics.Describe(desc)
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
	queueCounts := make(map[string]int, len(queueSizes))
	for i, count := range queueSizes {
		queueCounts[queues[i].Name] = int(count)
	}

	commmonmetrics.CollectQueueMetrics(queueCounts, c.queueMetrics, metrics)

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
			metrics <- prometheus.MustNewConstMetric(commmonmetrics.QueuePriorityDesc, prometheus.GaugeValue, priority.Priority, pool, queue.Name)
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
							commmonmetrics.QueueAllocatedDesc,
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
							commmonmetrics.QueueUsedDesc,
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
							commmonmetrics.QueueLeasedPodCountDesc,
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
						commmonmetrics.QueueAllocatedDesc,
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
						commmonmetrics.QueueUsedDesc,
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
						commmonmetrics.ClusterCapacityDesc,
						prometheus.GaugeValue,
						armadaresource.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						resourceType,
						nodeTypeUsage.NodeType.Id)
				}
				for resourceType, value := range nodeTypeUsage.AvailableCapacity {
					metrics <- prometheus.MustNewConstMetric(
						commmonmetrics.ClusterAvailableCapacity,
						prometheus.GaugeValue,
						armadaresource.QuantityAsFloat64(value),
						cluster,
						report.Pool,
						resourceType,
						nodeTypeUsage.NodeType.Id)
				}

				// Add metrics for the number of nodes and the number of nodes available to accept jobs
				metrics <- prometheus.MustNewConstMetric(
					commmonmetrics.ClusterCapacityDesc,
					prometheus.GaugeValue,
					float64(nodeTypeUsage.TotalNodes),
					cluster,
					report.Pool,
					"nodes",
					nodeTypeUsage.NodeType.Id)

				metrics <- prometheus.MustNewConstMetric(
					commmonmetrics.ClusterAvailableCapacity,
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
					commmonmetrics.ClusterCapacityDesc,
					prometheus.GaugeValue,
					armadaresource.QuantityAsFloat64(value),
					cluster,
					report.Pool,
					resourceType,
					report.Pool)
			}

			for resourceType, value := range report.ClusterAvailableCapacity {
				metrics <- prometheus.MustNewConstMetric(
					commmonmetrics.ClusterAvailableCapacity,
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
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.QueueSizeDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.QueuePriorityDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.QueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.QueueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.QueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MinQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MaxQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MedianQueueDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MinQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MaxQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MedianQueueResourcesDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.JobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MinJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MaxJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MedianJobRunDurationDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MinQueueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MaxQueueAllocatedDesc, e)
	metrics <- prometheus.NewInvalidMetric(commmonmetrics.MedianQueueAllocatedDesc, e)
}
