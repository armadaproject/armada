package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
)

const MetricPrefix = "armada_"

func ExposeDataMetrics(
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	usageRepository repository.UsageRepository,
) *QueueInfoCollector {
	collector := &QueueInfoCollector{
		queueRepository,
		jobRepository,
		usageRepository}
	prometheus.MustRegister(collector)
	return collector
}

type QueueInfoCollector struct {
	queueRepository repository.QueueRepository
	jobRepository   repository.JobRepository
	usageRepository repository.UsageRepository
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
	[]string{"queueName"},
	nil,
)

var queueUsageDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_usage",
	"Resource usage of a queue",
	[]string{"cluster", "queueName", "resourceType"},
	nil,
)

var clusterCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_capacity",
	"Cluster capacity",
	[]string{"cluster", "resourceType"},
	nil,
)

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- queueSizeDesc
	desc <- queuePriorityDesc
}

func (c *QueueInfoCollector) Collect(metrics chan<- prometheus.Metric) {

	queues, e := c.queueRepository.GetAllQueues()
	if e != nil {
		log.Errorf("Error while getting queue metrics %s", e)
		recordInvalidMetrics(metrics, e)
		return
	}

	queueSizes, e := c.jobRepository.GetQueueSizes(queues)
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

	queuePriority := scheduling.CalculateQueuesPriorityInfo(clusterPriorities, activeClusterReports, queues)

	for queue, priority := range queuePriority {
		metrics <- prometheus.MustNewConstMetric(queuePriorityDesc, prometheus.GaugeValue, priority.Priority, queue.Name)
	}

	for i, q := range queues {
		metrics <- prometheus.MustNewConstMetric(queueSizeDesc, prometheus.GaugeValue, float64(queueSizes[i]), q.Name)
	}

	for cluster, report := range activeClusterReports {
		for _, queueReport := range report.Queues {
			for resourceType, value := range queueReport.Resources {
				metrics <- prometheus.MustNewConstMetric(
					queueUsageDesc,
					prometheus.GaugeValue,
					common.QuantityAsFloat64(value),
					cluster,
					queueReport.Name,
					resourceType)
			}
		}
		for resourceType, value := range report.ClusterCapacity {
			metrics <- prometheus.MustNewConstMetric(
				clusterCapacityDesc,
				prometheus.GaugeValue,
				common.QuantityAsFloat64(value),
				cluster,
				resourceType)
		}
	}
}

func recordInvalidMetrics(metrics chan<- prometheus.Metric, e error) {
	metrics <- prometheus.NewInvalidMetric(queueSizeDesc, e)
	metrics <- prometheus.NewInvalidMetric(queuePriorityDesc, e)
	metrics <- prometheus.NewInvalidMetric(queueUsageDesc, e)
}
