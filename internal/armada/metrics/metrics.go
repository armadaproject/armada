package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common/util"
)

const metricPrefix = "armada_"

func ExposeDataMetrics(queueRepository repository.QueueRepository, jobRepository repository.JobRepository) *QueueInfoCollector {
	collector := &QueueInfoCollector{queueRepository, jobRepository, map[*api.Queue]float64{}}
	prometheus.MustRegister(collector)
	return collector
}

type MetricRecorder interface {
	RecordQueuePriorities(priorities map[*api.Queue]float64)
}

type QueueInfoCollector struct {
	queueRepository repository.QueueRepository
	jobRepository   repository.JobRepository
	priorities      map[*api.Queue]float64
}

var queueSizeDesc = prometheus.NewDesc(
	metricPrefix+"queue_size",
	"Number of jobs in a queue",
	[]string{"queueName"},
	nil,
)

var queuePriorityDesc = prometheus.NewDesc(
	metricPrefix+"queue_priority",
	"Priority of a queue",
	[]string{"queueName"},
	nil,
)

func (c *QueueInfoCollector) RecordQueuePriorities(priorities map[*api.Queue]float64) {
	c.priorities = priorities
}

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- queueSizeDesc
	desc <- queuePriorityDesc
}

func (c *QueueInfoCollector) Collect(metrics chan<- prometheus.Metric) {

	for queue, priority := range c.priorities {
		metrics <- prometheus.MustNewConstMetric(queuePriorityDesc, prometheus.GaugeValue, priority, queue.Name)
	}

	queues := util.GetPriorityMapQueues(c.priorities)

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
