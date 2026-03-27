package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordUncompressedEventSize(t *testing.T) {
	// Clean up: Unregister metrics to avoid polluting the global registry
	t.Cleanup(func() {
		prometheus.Unregister(uncompressedEventSize)
	})

	// Test happy path: increment with valid labels and size
	RecordUncompressedEventSize("job_submitted", "queue-1", 1024)
	RecordUncompressedEventSize("job_submitted", "queue-1", 2048)
	RecordUncompressedEventSize("job_running", "queue-2", 512)

	// Verify counter values
	value1 := testutil.ToFloat64(uncompressedEventSize.WithLabelValues("job_submitted", "queue-1"))
	assert.Equal(t, float64(3072), value1, "expected sum of 1024+2048 for job_submitted on queue-1")

	value2 := testutil.ToFloat64(uncompressedEventSize.WithLabelValues("job_running", "queue-2"))
	assert.Equal(t, float64(512), value2, "expected 512 for job_running on queue-2")
}

func TestRecordEstimatedCompressedEventSize(t *testing.T) {
	// Clean up: Unregister metrics to avoid polluting the global registry
	t.Cleanup(func() {
		prometheus.Unregister(estimatedCompressedEventSize)
	})

	// Test happy path: increment with valid labels and estimated size
	RecordEstimatedCompressedEventSize("job_submitted", "queue-1", 512.5)
	RecordEstimatedCompressedEventSize("job_submitted", "queue-1", 256.25)
	RecordEstimatedCompressedEventSize("job_running", "queue-2", 128.75)

	// Verify counter values
	value1 := testutil.ToFloat64(estimatedCompressedEventSize.WithLabelValues("job_submitted", "queue-1"))
	assert.Equal(t, 512.5+256.25, value1, "expected sum of 512.5+256.25 for job_submitted on queue-1")

	value2 := testutil.ToFloat64(estimatedCompressedEventSize.WithLabelValues("job_running", "queue-2"))
	assert.Equal(t, 128.75, value2, "expected 128.75 for job_running on queue-2")
}

func TestRecordBatchSize(t *testing.T) {
	// Clean up: Unregister metrics to avoid polluting the global registry
	t.Cleanup(func() {
		prometheus.Unregister(batchSize)
	})

	// Test happy path: increment with valid queue label and batch size
	RecordBatchSize("queue-1", 4096)
	RecordBatchSize("queue-1", 2048)
	RecordBatchSize("queue-2", 8192)

	// Verify counter values
	value1 := testutil.ToFloat64(batchSize.WithLabelValues("queue-1"))
	assert.Equal(t, float64(6144), value1, "expected sum of 4096+2048 for queue-1")

	value2 := testutil.ToFloat64(batchSize.WithLabelValues("queue-2"))
	assert.Equal(t, float64(8192), value2, "expected 8192 for queue-2")
}

func TestRecordBatchCompressionRatio(t *testing.T) {
	// Clean up: Unregister metrics to avoid polluting the global registry
	t.Cleanup(func() {
		prometheus.Unregister(batchCompressionRatio)
	})

	// Test happy path: increment with valid queue label and compression ratio
	RecordBatchCompressionRatio("queue-1", 5000, 10000) // 0.5 ratio
	RecordBatchCompressionRatio("queue-1", 3000, 10000) // 0.3 ratio
	RecordBatchCompressionRatio("queue-2", 8000, 10000) // 0.8 ratio

	// Verify counter values (sum of ratios)
	value1 := testutil.ToFloat64(batchCompressionRatio.WithLabelValues("queue-1"))
	assert.Equal(t, 0.5+0.3, value1, "expected sum of 0.5+0.3 ratios for queue-1")

	value2 := testutil.ToFloat64(batchCompressionRatio.WithLabelValues("queue-2"))
	assert.Equal(t, 0.8, value2, "expected 0.8 ratio for queue-2")

	// Test edge case: zero uncompressed size (should not increment)
	RecordBatchCompressionRatio("queue-3", 5000, 0)
	value3 := testutil.ToFloat64(batchCompressionRatio.WithLabelValues("queue-3"))
	assert.Equal(t, float64(0), value3, "expected 0 for queue-3 when uncompressed size is 0")
}

func TestRecordBatchCount(t *testing.T) {
	// Clean up: Unregister metrics to avoid polluting the global registry
	t.Cleanup(func() {
		prometheus.Unregister(batchCount)
	})

	// Test happy path: increment batch count per logical batch
	RecordBatchCount("queue-1")
	RecordBatchCount("queue-1")
	RecordBatchCount("queue-2")

	// Verify counter values
	value1 := testutil.ToFloat64(batchCount.WithLabelValues("queue-1"))
	assert.Equal(t, float64(2), value1, "expected 2 batches for queue-1")

	value2 := testutil.ToFloat64(batchCount.WithLabelValues("queue-2"))
	assert.Equal(t, float64(1), value2, "expected 1 batch for queue-2")
}
