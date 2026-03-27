package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
)

var m = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix)

func Get() *metrics.Metrics {
	return m
}

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    metrics.ArmadaEventIngesterMetricsPrefix + "write_duration_ms",
		Help:    "Duration of write to redis in milliseconds",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
	},
	[]string{"redis", "result"},
)

func RecordWriteDuration(redisName, result string, duration time.Duration) {
	requestDuration.
		With(map[string]string{"redis": redisName, "result": result}).
		Observe(float64(duration.Milliseconds()))
}

// uncompressedEventSize tracks the uncompressed size of individual events in bytes.
var uncompressedEventSize = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaEventIngesterMetricsPrefix + "uncompressed_event_size",
		Help: "Total uncompressed event size in bytes",
	},
	[]string{"event_type", "queue"},
)

// estimatedCompressedEventSize tracks the estimated compressed size of events (uncompressed * batch compression ratio).
var estimatedCompressedEventSize = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaEventIngesterMetricsPrefix + "estimated_compressed_event_size",
		Help: "Total estimated compressed event size in bytes",
	},
	[]string{"event_type", "queue"},
)

// batchSize tracks the compressed size of batches written to Redis in bytes.
var batchSize = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaEventIngesterMetricsPrefix + "batch_size",
		Help: "Total compressed batch size in bytes",
	},
	[]string{"queue"},
)

// batchCompressionRatio tracks the sum of compression ratios (compressed/uncompressed) across all batches.
var batchCompressionRatio = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaEventIngesterMetricsPrefix + "batch_compression_ratio",
		Help: "Sum of compression ratios (compressed/uncompressed) across batches; divide by batch_count for average ratio",
	},
	[]string{"queue"},
)

// batchCount tracks the number of logical batches written to Redis.
// Each logical batch is counted once (not per sink retry).
var batchCount = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaEventIngesterMetricsPrefix + "batch_count",
		Help: "Number of logical batches written to Redis (counted once, not per sink retry)",
	},
	[]string{"queue"},
)

// RecordUncompressedEventSize records the uncompressed size of an event.
// The increment is proto.Size(event) for the given event_type and queue.
func RecordUncompressedEventSize(eventType, queue string, sizeBytes uint64) {
	uncompressedEventSize.WithLabelValues(eventType, queue).Add(float64(sizeBytes))
}

// RecordEstimatedCompressedEventSize records the estimated compressed size of an event.
// The increment is uncompressedSize * batchCompressionRatio for the given event_type and queue.
func RecordEstimatedCompressedEventSize(eventType, queue string, estimatedSizeBytes float64) {
	estimatedCompressedEventSize.WithLabelValues(eventType, queue).Add(estimatedSizeBytes)
}

// RecordBatchSize records the compressed size of a batch written to Redis.
// The increment is the total compressed bytes of the batch for the given queue.
func RecordBatchSize(queue string, compressedBatchBytes uint64) {
	batchSize.WithLabelValues(queue).Add(float64(compressedBatchBytes))
}

// RecordBatchCompressionRatio records the compression ratio for a batch.
// The increment is compressedBatchBytes / uncompressedBatchBytes for the given queue.
// Note: Caller must ensure uncompressedBatchBytes > 0 to avoid division by zero.
func RecordBatchCompressionRatio(queue string, compressedBatchBytes, uncompressedBatchBytes uint64) {
	if uncompressedBatchBytes > 0 {
		ratio := float64(compressedBatchBytes) / float64(uncompressedBatchBytes)
		batchCompressionRatio.WithLabelValues(queue).Add(ratio)
	}
}

// RecordBatchCount records the writing of one logical batch to Redis.
// Each logical batch is counted once, regardless of how many times it is retried across sinks.
func RecordBatchCount(queue string) {
	batchCount.WithLabelValues(queue).Inc()
}

// UncompressedEventSizeMetric returns the uncompressed event size counter for testing.
func UncompressedEventSizeMetric() *prometheus.CounterVec {
	return uncompressedEventSize
}

// EstimatedCompressedEventSizeMetric returns the estimated compressed event size counter for testing.
func EstimatedCompressedEventSizeMetric() *prometheus.CounterVec {
	return estimatedCompressedEventSize
}
