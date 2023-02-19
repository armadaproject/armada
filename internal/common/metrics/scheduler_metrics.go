package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

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
		metrics <- NewQueueSizeMetric(count, q)
		queuedJobMetrics := metricsProvider.GetQueuedJobMetrics(q)
		runningJobMetrics := metricsProvider.GetRunningJobMetrics(q)
		for _, m := range queuedJobMetrics {
			queueDurations := m.Durations
			if queueDurations.GetCount() > 0 {
				metrics <- NewQueueDuration(m.Durations.GetCount(), queueDurations.GetSum(), queueDurations.GetBuckets(), m.Pool, m.PriorityClass, q)
				metrics <- NewMinQueueDuration(queueDurations.GetMin(), m.Pool, m.PriorityClass, q)
				metrics <- NewMaxQueueDuration(queueDurations.GetMax(), m.Pool, m.PriorityClass, q)
				metrics <- NewMedianQueueDuration(queueDurations.GetMedian(), m.Pool, m.PriorityClass, q)
			}

			// Sort the keys so we get a predicatable output order
			resources := maps.Keys(m.Resources)
			slices.Sort(resources)

			for _, resourceType := range resources {
				amount := m.Resources[resourceType]
				if amount.GetCount() > 0 {
					metrics <- NewQueueResources(amount.GetSum(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewMinQueueResources(amount.GetMin(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewMaxQueueResources(amount.GetMax(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewMedianQueueResources(amount.GetMedian(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewCountQueueResources(amount.GetCount(), m.Pool, m.PriorityClass, q, resourceType)
				}
			}
		}

		for _, m := range runningJobMetrics {
			runningJobDurations := m.Durations
			if runningJobDurations.GetCount() > 0 {
				metrics <- NewJobRunRunDuration(m.Durations.GetCount(), runningJobDurations.GetSum(), runningJobDurations.GetBuckets(), m.Pool, m.PriorityClass, q)
				metrics <- NewMinJobRunDuration(runningJobDurations.GetMin(), m.Pool, m.PriorityClass, q)
				metrics <- NewMaxJobRunDuration(runningJobDurations.GetMax(), m.Pool, m.PriorityClass, q)
				metrics <- NewMedianJobRunDuration(runningJobDurations.GetMedian(), m.Pool, m.PriorityClass, q)
			}

			// Sort the keys so we get a predicatable output order
			resources := maps.Keys(m.Resources)
			slices.Sort(resources)

			for _, resourceType := range resources {
				amount := m.Resources[resourceType]
				if amount.GetCount() > 0 {
					metrics <- NewMinQueueAllocated(amount.GetMin(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewMaxQueueAllocated(amount.GetMax(), m.Pool, m.PriorityClass, q, resourceType)
					metrics <- NewMedianQueueAllocated(amount.GetMedian(), m.Pool, m.PriorityClass, q, resourceType)
				}
			}
		}
	}
}

func NewQueueSizeMetric(value int, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueSizeDesc, prometheus.GaugeValue, float64(value), queue)
}

func NewQueuePriorityMetric(value float64, pool string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueuePriorityDesc, prometheus.GaugeValue, value, pool, queue)
}

func NewQueueDuration(count uint64, sum float64, buckets map[float64]uint64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstHistogram(QueueDurationDesc, count, sum, buckets, pool, priorityClass, queue)
}

func NewQueueResources(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewMaxQueueResources(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewMinQueueResources(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewMedianQueueResources(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewCountQueueResources(value uint64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(CountQueueResourcesDesc, prometheus.GaugeValue, float64(value), pool, priorityClass, queue, resource)
}

func NewMinQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMaxQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMedianQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMinJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMaxJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMedianJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewJobRunRunDuration(count uint64, sum float64, buckets map[float64]uint64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstHistogram(JobRunDurationDesc, count, sum, buckets, pool, priorityClass, queue)
}

func NewMinQueueAllocated(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewMaxQueueAllocated(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}

func NewMedianQueueAllocated(value float64, pool string, priorityClass string, queue string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, resource)
}
