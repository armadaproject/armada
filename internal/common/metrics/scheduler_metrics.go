package metrics

import (
	"regexp"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/pkg/bidstore"
)

const MetricPrefix = "armada_"
const queuedPhase = "queued"
const runningPhase = "running"

var QueueSizeDesc = prometheus.NewDesc(
	MetricPrefix+"queue_size",
	"Number of jobs in a queue",
	[]string{"queueName", "queue"},
	nil,
)

var QueueDistinctSchedulingKeysDesc = prometheus.NewDesc(
	MetricPrefix+"queue_distinct_scheduling_keys",
	"Number of distinct scheduling keys requested by a queue",
	[]string{"queueName", "queue"},
	nil,
)

var QueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued",
	"Resource required by queued jobs",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MinQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_min",
	"Min resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MaxQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_max",
	"Max resource required by queued job",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MedianQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_median",
	"Median resource required by queued jobs",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var CountQueueResourcesDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_queued_count",
	"Count of queued jobs requiring resource",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MinQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_min",
	"Min queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var MaxQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_max",
	"Max queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var MedianQueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds_median",
	"Median queue time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var QueueDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_queued_seconds",
	"Queued time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var MinJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_min",
	"Min run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var MaxJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_max",
	"Max run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var MedianJobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds_median",
	"Median run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var JobRunDurationDesc = prometheus.NewDesc(
	MetricPrefix+"job_run_time_seconds",
	"Run time for Armada jobs",
	[]string{"pool", "priorityClass", "queueName", "queue"},
	nil,
)

var QueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated",
	"Resource allocated to running jobs of a queue",
	[]string{"cluster", "pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType", "nodeType"},
	nil,
)

var MinQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_min",
	"Min resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MaxQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_max",
	"Max resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var MedianQueueAllocatedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_allocated_median",
	"Median resource allocated by a running job",
	[]string{"pool", "priorityClass", "queueName", "queue", "priceBand", "resourceType"},
	nil,
)

var QueueUsedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_resource_used",
	"Resource actually being used by running jobs of a queue",
	[]string{"cluster", "pool", "queueName", "queue", "resourceType", "nodeType"},
	nil,
)

var QueueLeasedPodCountDesc = prometheus.NewDesc(
	MetricPrefix+"queue_leased_pod_count",
	"Number of leased pods",
	[]string{"cluster", "pool", "queueName", "queue", "phase", "nodeType"},
	nil,
)

var ClusterCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_capacity",
	"Cluster capacity",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var ClusterFarmCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_farm_capacity",
	"Cluster capacity less usage from non-Armada pods",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var ClusterAvailableCapacityDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_available_capacity",
	"Cluster capacity available for Armada jobs",
	[]string{"cluster", "pool", "resourceType", "nodeType"},
	nil,
)

var ClusterCordonedStatusDesc = prometheus.NewDesc(
	MetricPrefix+"cluster_cordoned_status",
	"Cluster cordoned status",
	[]string{"cluster", "reason", "setByUser"},
	nil,
)

var QueuePriorityDesc = prometheus.NewDesc(
	MetricPrefix+"queue_priority",
	"Queue priority factor",
	[]string{"queueName", "queue"},
	nil,
)

var MinQueuePriceQueuedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_queued_min",
	"Minimum price of queued jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var MaxQueuePriceQueuedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_queued_max",
	"Maximum price of queued jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var MedianQueuePriceQueuedDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_queued_median",
	"Median price of queued jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var MinQueuePriceRunningDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_running_min",
	"Minimum price of running jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var MaxQueuePriceRunningDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_running_max",
	"Maximum price of running jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var MedianQueuePriceRunningDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_running_median",
	"Median price of running jobs",
	[]string{"pool", "priorityClass", "queue"},
	nil,
)

var QueuePriceBandPhaseBidDesc = prometheus.NewDesc(
	MetricPrefix+"queue_price_band_phase_bid",
	"Bid price for a queues price band",
	[]string{"pool", "queueName", "queue", "phase", "priceBand"},
	nil,
)

var (
	queueLabelMetricName        = MetricPrefix + "queue_labels"
	queueLabelMetricDescription = "Queue labels"
	queueLabelDefaultLabels     = []string{"queueName", "queue"}
)

// QueueLabelDesc so it can be added to AllDescs which makes Describe() work properly
//
//	actual describe for this metric is generated dynamically as the labels are dynamic
var QueueLabelDesc = prometheus.NewDesc(
	queueLabelMetricName,
	queueLabelMetricDescription,
	queueLabelDefaultLabels,
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
	ClusterFarmCapacityDesc,
	ClusterAvailableCapacityDesc,
	QueuePriorityDesc,
	QueueLabelDesc,
	QueuePriceBandPhaseBidDesc,
}

func Describe(out chan<- *prometheus.Desc) {
	for _, desc := range AllDescs {
		out <- desc
	}
}

func CollectQueueMetrics(pools []configuration.PoolConfig, queueCounts map[string]int, bidPriceSnapshot pricing.BidPriceSnapshot, queueDistinctSchedulingKeyCounts map[string]int, metricsProvider QueueMetricProvider) []prometheus.Metric {
	metrics := make([]prometheus.Metric, 0, len(AllDescs))
	poolNames := armadaslices.Map(pools, func(pool configuration.PoolConfig) string {
		return pool.Name
	})

	for q, count := range queueCounts {
		metrics = append(metrics, NewQueueSizeMetric(count, q))
		metrics = append(metrics, NewQueueDistinctSchedulingKeyMetric(queueDistinctSchedulingKeyCounts[q], q))
		queuedJobMetrics := metricsProvider.GetQueuedJobMetrics(q)
		runningJobMetrics := metricsProvider.GetRunningJobMetrics(q)
		for priceBandShortName, priceBand := range bidstore.PriceBandFromShortName {
			bidsByPool, exists := bidPriceSnapshot.GetPrice(q, priceBand)
			for _, pool := range poolNames {
				if bid, poolBidExists := bidsByPool[pool]; !exists || !poolBidExists {
					metrics = append(metrics, NewQueuePriceBandBidMetric(0, pool, q, queuedPhase, priceBandShortName))
					metrics = append(metrics, NewQueuePriceBandBidMetric(0, pool, q, runningPhase, priceBandShortName))
				} else {
					metrics = append(metrics, NewQueuePriceBandBidMetric(bid.QueuedBid, pool, q, queuedPhase, priceBandShortName))
					metrics = append(metrics, NewQueuePriceBandBidMetric(bid.RunningBid, pool, q, runningPhase, priceBandShortName))
				}
			}

			if exists {
				for pool, bid := range bidsByPool {
					metrics = append(metrics, NewQueuePriceBandBidMetric(bid.QueuedBid, pool, q, queuedPhase, priceBandShortName))
					metrics = append(metrics, NewQueuePriceBandBidMetric(bid.RunningBid, pool, q, runningPhase, priceBandShortName))
				}
			}
		}
		for _, m := range queuedJobMetrics {
			queueDurations := m.Durations
			if queueDurations.GetCount() > 0 {
				metrics = append(metrics, NewQueueDuration(m.Durations.GetCount(), queueDurations.GetSum(), queueDurations.GetBuckets(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMinQueueDuration(queueDurations.GetMin(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMaxQueueDuration(queueDurations.GetMax(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMedianQueueDuration(queueDurations.GetMedian(), m.Pool, m.PriorityClass, q))
			}

			metrics = append(metrics, NewMinQueuePriceQueuedMetric(m.BidPrices.GetMin(), m.Pool, m.PriorityClass, q))
			metrics = append(metrics, NewMaxQueuePriceQueuedMetric(m.BidPrices.GetMax(), m.Pool, m.PriorityClass, q))
			metrics = append(metrics, NewMedianQueuePriceQueuedMetric(m.BidPrices.GetMedian(), m.Pool, m.PriorityClass, q))

			// Sort the keys so we get a predictable output order
			resourcePriceBands := maps.Keys(m.Resources)
			slices.Sort(resourcePriceBands)

			for _, priceBand := range resourcePriceBands {
				priceBandShortName := GetPriceBandShortName(priceBand)
				resourceKeys := maps.Keys(m.Resources[priceBand])
				// Sort the keys so we get a predictable output order
				slices.Sort(resourceKeys)
				for _, resourceType := range resourceKeys {
					amount := m.Resources[priceBand][resourceType]
					if amount.GetCount() > 0 {
						metrics = append(metrics, NewQueueResources(amount.GetSum(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewMinQueueResources(amount.GetMin(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewMaxQueueResources(amount.GetMax(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewMedianQueueResources(amount.GetMedian(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewCountQueueResources(amount.GetCount(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
					}
				}
			}

		}

		for _, m := range runningJobMetrics {
			runningJobDurations := m.Durations
			if runningJobDurations.GetCount() > 0 {
				metrics = append(metrics, NewJobRunRunDuration(m.Durations.GetCount(), runningJobDurations.GetSum(), runningJobDurations.GetBuckets(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMinJobRunDuration(runningJobDurations.GetMin(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMaxJobRunDuration(runningJobDurations.GetMax(), m.Pool, m.PriorityClass, q))
				metrics = append(metrics, NewMedianJobRunDuration(runningJobDurations.GetMedian(), m.Pool, m.PriorityClass, q))
			}

			metrics = append(metrics, NewMinQueuePriceRunningMetric(m.BidPrices.GetMin(), m.Pool, m.PriorityClass, q))
			metrics = append(metrics, NewMaxQueuePriceRunningMetric(m.BidPrices.GetMax(), m.Pool, m.PriorityClass, q))
			metrics = append(metrics, NewMedianQueuePriceRunningMetric(m.BidPrices.GetMedian(), m.Pool, m.PriorityClass, q))

			// Sort the keys so we get a predictable output order
			resourcePriceBands := maps.Keys(m.Resources)
			slices.Sort(resourcePriceBands)

			for _, priceBand := range resourcePriceBands {
				priceBandShortName := GetPriceBandShortName(priceBand)
				resourceKeys := maps.Keys(m.Resources[priceBand])
				// Sort the keys so we get a predictable output order
				slices.Sort(resourceKeys)
				for _, resourceType := range resourceKeys {
					amount := m.Resources[priceBand][resourceType]
					if amount.GetCount() > 0 {
						metrics = append(metrics, NewMinQueueAllocated(amount.GetMin(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewMaxQueueAllocated(amount.GetMax(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
						metrics = append(metrics, NewMedianQueueAllocated(amount.GetMedian(), m.Pool, m.PriorityClass, q, priceBandShortName, resourceType))
					}
				}
			}
		}
	}
	for _, queue := range metricsProvider.GetAllQueues() {
		metrics = append(metrics, NewQueuePriorityMetric(queue.PriorityFactor, queue.Name))
		metrics = append(metrics, NewQueueLabelsMetric(queue.Name, queue.Labels))
	}
	return metrics
}

func NewQueueSizeMetric(value int, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueSizeDesc, prometheus.GaugeValue, float64(value), queue, queue)
}

func NewQueueDistinctSchedulingKeyMetric(value int, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueDistinctSchedulingKeysDesc, prometheus.GaugeValue, float64(value), queue, queue)
}

func NewQueueDuration(count uint64, sum float64, buckets map[float64]uint64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstHistogram(QueueDurationDesc, count, sum, buckets, pool, priorityClass, queue, queue)
}

func NewQueueResources(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, priceBand, queue, resource)
}

func NewMaxQueueResources(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewMinQueueResources(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewMedianQueueResources(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueResourcesDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewCountQueueResources(value uint64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(CountQueueResourcesDesc, prometheus.GaugeValue, float64(value), pool, priorityClass, queue, queue, priceBand, resource)
}

func NewMinQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewMaxQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewMedianQueueDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewMinJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewMaxJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewMedianJobRunDuration(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianJobRunDurationDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue)
}

func NewJobRunRunDuration(count uint64, sum float64, buckets map[float64]uint64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstHistogram(JobRunDurationDesc, count, sum, buckets, pool, priorityClass, queue, queue)
}

func NewMinQueueAllocated(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewMaxQueueAllocated(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewMedianQueueAllocated(value float64, pool string, priorityClass string, queue string, priceBand string, resource string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueueAllocatedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue, queue, priceBand, resource)
}

func NewQueueAllocated(value float64, queue string, cluster string, pool string, priorityClass string, priceBand string, resource string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueAllocatedDesc, prometheus.GaugeValue, value, cluster, pool, priorityClass, queue, queue, priceBand, resource, nodeType)
}

func NewQueueLeasedPodCount(value float64, cluster string, pool string, queue string, phase string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueLeasedPodCountDesc, prometheus.GaugeValue, value, cluster, pool, queue, queue, phase, nodeType)
}

func NewClusterAvailableCapacity(value float64, cluster string, pool string, resource string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(ClusterAvailableCapacityDesc, prometheus.GaugeValue, value, cluster, pool, resource, nodeType)
}

func NewClusterFarmCapacity(value float64, cluster string, pool string, resource string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(ClusterFarmCapacityDesc, prometheus.GaugeValue, value, cluster, pool, resource, nodeType)
}

func NewClusterTotalCapacity(value float64, cluster string, pool string, resource string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(ClusterCapacityDesc, prometheus.GaugeValue, value, cluster, pool, resource, nodeType)
}

func NewClusterCordonedStatus(value float64, cluster string, reason string, setByUser string) prometheus.Metric {
	return prometheus.MustNewConstMetric(ClusterCordonedStatusDesc, prometheus.GaugeValue, value, cluster, reason, setByUser)
}

func NewQueueUsed(value float64, queue string, cluster string, pool string, resource string, nodeType string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueueUsedDesc, prometheus.GaugeValue, value, cluster, pool, queue, queue, resource, nodeType)
}

func NewQueuePriorityMetric(value float64, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueuePriorityDesc, prometheus.GaugeValue, value, queue, queue)
}

func NewMinQueuePriceQueuedMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueuePriceQueuedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMaxQueuePriceQueuedMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueuePriceQueuedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMedianQueuePriceQueuedMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueuePriceQueuedDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMinQueuePriceRunningMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MinQueuePriceRunningDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMaxQueuePriceRunningMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MaxQueuePriceRunningDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewMedianQueuePriceRunningMetric(value float64, pool string, priorityClass string, queue string) prometheus.Metric {
	return prometheus.MustNewConstMetric(MedianQueuePriceRunningDesc, prometheus.GaugeValue, value, pool, priorityClass, queue)
}

func NewQueuePriceBandBidMetric(value float64, pool string, queue string, phase, priceBand string) prometheus.Metric {
	return prometheus.MustNewConstMetric(QueuePriceBandPhaseBidDesc, prometheus.GaugeValue, value, pool, queue, queue, phase, priceBand)
}

func NewQueueLabelsMetric(queue string, labels map[string]string) prometheus.Metric {
	metricLabels := make([]string, 0, len(labels)+len(queueLabelDefaultLabels))
	values := make([]string, 0, len(labels)+len(queueLabelDefaultLabels))

	metricLabels = append(metricLabels, queueLabelDefaultLabels...)
	values = append(values, queue)
	values = append(values, queue)

	for key, value := range labels {
		if isValidMetricLabelName(key) {
			metricLabels = append(metricLabels, key)
			values = append(values, value)
		}
	}

	queueLabelsDesc := prometheus.NewDesc(
		queueLabelMetricName,
		queueLabelMetricDescription,
		metricLabels,
		nil,
	)

	return prometheus.MustNewConstMetric(queueLabelsDesc, prometheus.GaugeValue, 1, values...)
}

func isValidMetricLabelName(labelName string) bool {
	// Prometheus metric label names must match the following regex: [a-zA-Z_][a-zA-Z0-9_]*
	// See: https://prometheus.io/docs/concepts/data_model/
	match, _ := regexp.MatchString("^[a-zA-Z_][a-zA-Z0-9_]*$", labelName)
	return match
}
