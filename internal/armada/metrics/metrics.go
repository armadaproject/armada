package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
)

const metricPrefix = "armada_"

func ExposeDataMetrics(queueRepository repository.QueueRepository, jobRepository repository.JobRepository) *QueueInfoCollector {
	collector := &QueueInfoCollector{queueRepository, jobRepository, map[*api.Queue]scheduling.QueuePriorityInfo{}}
	prometheus.MustRegister(collector)
	return collector
}

type MetricRecorder interface {
	RecordQueuePriorities(priorities map[*api.Queue]scheduling.QueuePriorityInfo)
}

type QueueInfoCollector struct {
	queueRepository repository.QueueRepository
	jobRepository   repository.JobRepository
	priorities      map[*api.Queue]scheduling.QueuePriorityInfo
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

func (c *QueueInfoCollector) RecordQueuePriorities(priorities map[*api.Queue]scheduling.QueuePriorityInfo) {
	c.priorities = priorities
}

func (c *QueueInfoCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- queueSizeDesc
	desc <- queuePriorityDesc
}

func (c *QueueInfoCollector) Collect(metrics chan<- prometheus.Metric) {

	for queue, priority := range c.priorities {
		metrics <- prometheus.MustNewConstMetric(queuePriorityDesc, prometheus.GaugeValue, priority.Priority, queue.Name)
	}

	queues := scheduling.GetPriorityMapQueues(c.priorities)

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
