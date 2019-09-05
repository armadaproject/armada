package metrics

import (
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const metricPrefix = "armada_"

func ExposeDataMetrics(queueRepository repository.QueueRepository, jobRepository repository.JobRepository) {
	prometheus.MustRegister(&QueueInfoCollector{queueRepository, jobRepository})
}

type QueueInfoCollector struct {
	queueRepository repository.QueueRepository
	jobRepository   repository.JobRepository
}

var queueSizeDesc = prometheus.NewDesc(
	metricPrefix+"queue_size",
	"Number of jobs in a queue",
	[]string{"queueName"},
	nil,
)

var queuePriorityDesc = prometheus.NewDesc(
	metricPrefix+"queue_Priority",
	"Priority of a queue",
	[]string{"queueName"},
	nil,
)

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- queueSizeDesc
	desc <- queuePriorityDesc
}

func (c *QueueInfoCollector) Collect(metrics chan<- prometheus.Metric) {

	queues, e := c.queueRepository.GetQueues()
	if e != nil {
		log.Errorf("Error while getting queue metrics %s", e)
		metrics <- prometheus.NewInvalidMetric(queuePriorityDesc, e)
		return
	}
	for _, q := range queues {
		metrics <- prometheus.MustNewConstMetric(queuePriorityDesc, prometheus.GaugeValue, q.Priority, q.Name)
	}

	queueSizes, e := c.jobRepository.GetQueueSizes(queues)
	if e != nil {
		log.Errorf("Error while getting queue size metrics %s", e)
		metrics <- prometheus.NewInvalidMetric(queueSizeDesc, e)
		return
	}

	for i, q := range queues {
		metrics <- prometheus.MustNewConstMetric(queueSizeDesc, prometheus.GaugeValue, float64(queueSizes[i]), q.Name)

	}
}
