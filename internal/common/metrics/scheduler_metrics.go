package metrics

import "github.com/prometheus/client_golang/prometheus"

const MetricPrefix = "armada_"

var QueueSizeDesc = prometheus.NewDesc(
	MetricPrefix+"queue_size",
	"Number of jobs in a queue",
	[]string{"queueName"},
	nil,
)

var QueuePriorityDesc = prometheus.NewDesc(
	MetricPrefix+"queue_priority",
	"Priority of a queue",
	[]string{"pool", "queueName"},
	nil,
)

var QueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued",
	"Resource required by queued jobs",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MinQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_min",
	"Min resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MaxQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_max",
	"Max resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MedianQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_median",
	"Median resource required by queued jobs",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var CountQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_count",
	"Count of queued jobs requiring resource",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MinQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_min",
	"Min queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var MaxQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_max",
	"Max queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var MedianQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_median",
	"Median queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var QueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds",
	"Queued time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var MinJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_min",
	"Min run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var MaxJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_max",
	"Max run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var MedianJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_median",
	"Median run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var JobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds",
	"Run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName"},
	nil,
)

var QueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated",
	"Resource allocated to running jobs of a queue",
	[]string{"cluster", "pool", "queueName", "resourceType", "nodeType"},
	nil,
)

var MinQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_min",
	"Min resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MaxQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_max",
	"Max resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var MedianQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_median",
	"Median resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "resourceType"},
	nil,
)

var QueueUsedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_used",
	"Resource actually being used by running jobs of a queue",
	[]string{"cluster", "pool", "queueName", "resourceType", "nodeType"},
	nil,
)

var QueueLeasedPodCountDesc = prometheus.NewDesc(
	MetricPrefix+"queue_leased_pod_count",
	"Number of leased pods",
	[]string{"cluster", "pool", "queueName", "phase", "nodeType"},
	nil,
)

var ClusterCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_capacity",
	"Cluster capacity",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var ClusterAvailableCapacity = prometheus.NewDesc(
	MetricPrefix+"cluster_available_capacity",
	"Cluster capacity available for Armada jobs",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var AllDescs = []*prometheus.Desc{
	QueueSizeDesc,
	QueuePriorityDesc,
	QueueResourcesDesc,
	MinQueueResourcesDesc,
	MaxQueueResourcesDesc,
	MedianQueueResourcesDesc,
	CountQueueResourcesDesc,
	MinQueueDurationDesc,
	MaxQueueDurationDesc,
	MedianQueueDurationDesc,
	MedianQueueDurationDesc,
	QueueDurationDesc,
	MinJobRunDurationDesc,
	MaxJobRunDurationDesc,
	MedianJobRunDurationDesc,
	JobRunDurationDesc,
	QueueAllocatedDesc,
	MinQueueAllocatedDesc,
	MaxQueueAllocatedDesc,
	MedianQueueAllocatedDesc,
	QueueUsedDesc,
	QueueLeasedPodCountDesc,
	ClusterCapacityDesc,
	ClusterAvailableCapacity,
}

func Describe(out chan<- *prometheus.Desc) {
	for _, desc := range AllDescs {
		out <- desc
	}
}

func CollectQueueMetrics(queueCounts map[string]int, metricsProvider QueueMetricProvider, metrics chan<- prometheus.Metric) {
	for q, count := range queueCounts {
		metrics <- prometheus.MustNewConstMetric(QueueSizeDesc, prometheus.GaugeValue, float64(count), q)
		queuedJobMetrics := metricsProvider.GetQueuedJobMetrics(q)
		runningJobMetrics := metricsProvider.GetRunningJobMetrics(q)
		for _, m := range queuedJobMetrics {
			queueDurations := m.Durations
			if queueDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(QueueDurationDesc, m.Durations.GetCount(),
					queueDurations.GetSum(), queueDurations.GetBuckets(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MinQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMin(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MaxQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMax(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MedianQueueDurationDesc, prometheus.GaugeValue, queueDurations.GetMedian(), m.Pool, m.PriorityClass, q)
			}

			for resourceType, amount := range m.Resources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(QueueResourcesDesc, prometheus.GaugeValue, amount.GetSum(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(MinQueueResourcesDesc, prometheus.GaugeValue, amount.GetMin(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(MaxQueueResourcesDesc, prometheus.GaugeValue, amount.GetMax(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(MedianQueueResourcesDesc, prometheus.GaugeValue, amount.GetMedian(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(CountQueueResourcesDesc, prometheus.GaugeValue, float64(amount.GetCount()), m.Pool, m.PriorityClass, q, resourceType)
				}
			}
		}

		for _, m := range runningJobMetrics {
			runningJobDurations := m.Durations
			if runningJobDurations.GetCount() > 0 {
				metrics <- prometheus.MustNewConstHistogram(JobRunDurationDesc, runningJobDurations.GetCount(),
					runningJobDurations.GetSum(), runningJobDurations.GetBuckets(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MinJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMin(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MaxJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMax(), m.Pool, m.PriorityClass, q)
				metrics <- prometheus.MustNewConstMetric(MedianJobRunDurationDesc, prometheus.GaugeValue, runningJobDurations.GetMedian(), m.Pool, m.PriorityClass, q)
			}

			for resourceType, amount := range m.Resources {
				if amount.GetCount() > 0 {
					metrics <- prometheus.MustNewConstMetric(MinQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMin(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(MaxQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMax(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- prometheus.MustNewConstMetric(MedianQueueAllocatedDesc, prometheus.GaugeValue, amount.GetMedian(), m.Pool, m.PriorityClass, q, resourceType)
				}
			}
		}
	}
}
