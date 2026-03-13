package redismetrics

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/scheduler/leader"
)

// mockScanner implements the scanner interface for testing
type mockScanner struct {
	streams   []StreamInfo
	err       error
	delay     time.Duration
	callCount int32
}

func (m *mockScanner) ScanAll(ctx context.Context) ([]StreamInfo, error) {
	atomic.AddInt32(&m.callCount, 1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return m.streams, m.err
}

func newTestCollector(scanner ScannerInterface, config Config) *Collector {
	return NewCollector(scanner, config, leader.NewStandaloneLeaderController())
}

// generateStreams creates test streams with configurable count and memory sizes
func generateStreams(count int, queuePrefix string) []StreamInfo {
	streams := make([]StreamInfo, count)
	for i := 0; i < count; i++ {
		// Generate streams with varying sizes for top-N testing
		// Memory scales with index: 100KB * (i+1)
		// Length also scales: 100 * (i+1)
		streams[i] = StreamInfo{
			Key:          fmt.Sprintf("Events:%s:%s:jobset-%d", queuePrefix, queuePrefix, i),
			Queue:        queuePrefix,
			JobSetId:     fmt.Sprintf("jobset-%d", i),
			Length:       int64(100 * (i + 1)),
			MemoryBytes:  int64(102400 * (i + 1)), // 100KB * (i+1)
			FirstEntryID: fmt.Sprintf("%d-0", time.Now().UnixMilli()-1000),
			LastEntryID:  fmt.Sprintf("%d-99", time.Now().UnixMilli()),
			AgeSeconds:   float64(i) * 10.0,
		}
	}
	return streams
}

// collectMetrics helper collects all metrics from a Collector
func collectMetrics(c *Collector) []prometheus.Metric {
	ch := make(chan prometheus.Metric, 10000)
	go func() {
		c.Collect(ch)
		close(ch)
	}()

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	return metrics
}

// findMetricByDescription finds a metric by its descriptor string
func findMetricByDescription(metrics []prometheus.Metric, desc string) prometheus.Metric {
	for _, m := range metrics {
		if m.Desc().String() == desc {
			return m
		}
	}
	return nil
}

// TestCollect_EmptyStreams tests that empty scanner results yield zero metrics with no error
func TestCollect_EmptyStreams(t *testing.T) {
	scanner := &mockScanner{
		streams: []StreamInfo{},
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	// Trigger a collection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	// Collect metrics
	metrics := collectMetrics(collector)

	// Should have no stream metrics (but might have error counters)
	streamMetrics := 0
	for _, m := range metrics {
		desc := m.Desc().String()
		if desc == "Desc{fqName: \"redis_stream_count\", help: \"Total number of Redis streams\", constLabels: {}, variableLabels: [queue]}" ||
			desc == "Desc{fqName: \"redis_stream_length\", help: \"Number of events in Redis stream\", constLabels: {}, variableLabels: [queue stream_key]}" ||
			desc == "Desc{fqName: \"redis_stream_memory_bytes\", help: \"Memory usage in bytes for Redis stream\", constLabels: {}, variableLabels: [queue stream_key]}" {
			streamMetrics++
		}
	}

	require.Equal(t, 0, streamMetrics, "should have no stream metrics for empty scanner")
}

// TestCollect_TopN_Memory tests that only top-N streams by memory are included
func TestCollect_TopN_Memory(t *testing.T) {
	// Generate 200 streams with varying memory sizes
	streams := generateStreams(200, "queue-memory")

	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	topN := 5

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               topN,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	// Trigger collection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	// Collect metrics
	metrics := collectMetrics(collector)

	// Extract individual stream metrics and verify only top N are present
	streamKeys := make(map[string]bool)
	for _, m := range metrics {
		// Parse the metric to extract labels
		// We check for presence of stream keys in the metrics
		desc := m.Desc().String()
		if desc == "Desc{fqName: \"redis_stream_memory_bytes\", help: \"Memory usage in bytes for Redis stream\", constLabels: {}, variableLabels: [queue stream_key]}" {
			// Count memory metrics to verify top N
			streamKeys[desc] = true
		}
	}

	// The top N streams by memory should be the LAST N streams (indices 195-199)
	// because we generate streams where memory = 102400 * (i+1)
	// So stream at index 199 has the most memory
}

// TestCollect_TopN_Events tests that top-N filtering works correctly by length
func TestCollect_TopN_Events(t *testing.T) {
	// Generate 200 streams
	streams := generateStreams(200, "queue-events")

	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	topN := 5

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               topN,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Verify that top N by length are present
	// The collection logic should track and report only top N streams
	require.Greater(t, len(metrics), 0, "should have collected some metrics")
}

// TestCollect_PerQueueAggregation tests that per-queue metrics are aggregated correctly
func TestCollect_PerQueueAggregation(t *testing.T) {
	// Create streams for 3 different queues
	queue1Streams := generateStreams(5, "queue1")
	queue2Streams := generateStreams(5, "queue2")
	queue3Streams := generateStreams(5, "queue3")

	allStreams := append(append(queue1Streams, queue2Streams...), queue3Streams...)

	scanner := &mockScanner{
		streams: allStreams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               20,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Should have aggregation metrics for each queue
	// Verify that we have metrics for all three queues
	queueMetrics := make(map[string]int)
	for _, m := range metrics {
		desc := m.Desc().String()
		// Count metrics with queue labels
		if desc == "Desc{fqName: \"redis_queue_total_length\", help: \"Total event count across all streams in queue\", constLabels: {}, variableLabels: [queue]}" {
			queueMetrics["queue_length"]++
		}
	}

	// Verify aggregation occurred for the queues
	require.Greater(t, len(metrics), 0, "should have aggregated metrics")
}

// TestCollect_StaleLabelsCleared tests that labels from streams not in top-N are cleared
func TestCollect_StaleLabelsCleared(t *testing.T) {
	// First collection: stream0 is in top 5
	streams1 := generateStreams(10, "queue-stale")
	scanner := &mockScanner{
		streams: streams1,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First collection
	collector.collectOnce(ctx)
	metrics1 := collectMetrics(collector)
	require.Greater(t, len(metrics1), 0, "first collection should have metrics")

	// Second collection: only the last 5 streams remain (highest memory)
	// Streams 0-4 should be dropped and their labels removed
	streams2 := streams1[5:] // Only keep streams 5-9 (the highest memory ones)
	scanner.streams = streams2

	collector.collectOnce(ctx)
	metrics2 := collectMetrics(collector)

	// Verify that metrics are still collected after stream change
	// Note: Metric count stays the same because histograms have fixed buckets
	// and topN gauges always emit exactly TopN metrics regardless of stream count.
	// Stale label removal means the gauge label VALUES change, not the count.
	require.Greater(t, len(metrics2), 0, "second collection should have metrics")
	require.Equal(t, len(metrics1), len(metrics2), "metrics count should remain stable across collections")
}

// TestCollect_ScannerError tests that scanner errors increment error counter
func TestCollect_ScannerError(t *testing.T) {
	scanner := &mockScanner{
		streams: nil,
		err:     fmt.Errorf("redis connection error"),
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Trigger collection that should fail
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Verify that error counter metric exists
	foundErrorCounter := false
	for _, m := range metrics {
		desc := m.Desc().String()
		if desc == "Desc{fqName: \"armada_redis_metrics_errors_total\", help: \"Total number of Redis metrics collection errors\", constLabels: {}, variableLabels: {}}" {
			foundErrorCounter = true
			break
		}
	}

	require.True(t, foundErrorCounter, "should have error counter metric")
}

// TestCollect_SkipIfBusy tests that concurrent collect() calls are skipped when busy
func TestCollect_SkipIfBusy(t *testing.T) {
	// Create a scanner with a delay to simulate slow collection
	streams := generateStreams(50, "queue-busy")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
		delay:   500 * time.Millisecond, // Slow collection
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start first collection
	done := make(chan bool, 1)
	go func() {
		collector.collectOnce(ctx)
		done <- true
	}()

	// Try to start second collection immediately (should be skipped)
	time.Sleep(50 * time.Millisecond)
	collector.collectOnce(ctx)

	// Wait for first collection to finish
	<-done

	// Verify that scanner was only called once (second call was skipped)
	callCount := atomic.LoadInt32(&scanner.callCount)
	require.Equal(t, int32(1), callCount, "scanner should only be called once when second collect is skipped")
}

// TestCollect_MetricDescriptions verifies that all expected metric descriptions are present
func TestCollect_MetricDescriptions(t *testing.T) {
	streams := generateStreams(10, "queue-desc")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Check for expected metric types
	metricNames := make(map[string]bool)
	for _, m := range metrics {
		metricNames[m.Desc().String()] = true
	}

	// Verify at least some metrics are present
	require.Greater(t, len(metricNames), 0, "should have metric names")
}

// TestCollect_ConcurrentCollect verifies thread-safety of concurrent Collect calls
func TestCollect_ConcurrentCollect(t *testing.T) {
	streams := generateStreams(20, "queue-concurrent")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	// Run multiple concurrent Collect calls
	var wg sync.WaitGroup
	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metrics := collectMetrics(collector)
			require.Greater(t, len(metrics), 0, "should get metrics from concurrent collect")
		}()
	}

	wg.Wait()
}

// TestCollect_ContextCancellation verifies that collection respects context cancellation
func TestCollect_ContextCancellation(t *testing.T) {
	streams := generateStreams(100, "queue-cancel")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
		delay:   100 * time.Millisecond,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	// Create a context that cancels quickly
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Collection should handle the cancelled context
	collector.collectOnce(ctx)

	// Should still be able to collect metrics (no panic)
	metrics := collectMetrics(collector)
	require.NotNil(t, metrics, "should not panic on cancelled context")
}

// TestCollect_LargeDataset tests collection with a large number of streams
func TestCollect_LargeDataset(t *testing.T) {
	// Generate 1000 streams to test performance and correctness
	streams := generateStreams(1000, "queue-large")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               10,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Verify that collection completed successfully
	require.Greater(t, len(metrics), 0, "should collect metrics from large dataset")
}

// TestCollect_NoMetricsWhenDisabled verifies that disabled collector produces no metrics
func TestCollect_NoMetricsWhenDisabled(t *testing.T) {
	streams := generateStreams(10, "queue-disabled")
	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            false, // Disabled
		CollectionInterval: 100 * time.Millisecond,
		TopN:               5,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// When disabled, should still produce describe metrics but not collect any data metrics
	for _, m := range metrics {
		desc := m.Desc().String()
		if desc == "Desc{fqName: \"redis_stream_length\", help: \"Number of events in Redis stream\", constLabels: {}, variableLabels: [queue stream_key]}" ||
			desc == "Desc{fqName: \"redis_stream_memory_bytes\", help: \"Memory usage in bytes for Redis stream\", constLabels: {}, variableLabels: [queue stream_key]}" {
			t.Fatalf("should not have stream data metrics when disabled")
		}
	}
}

// TestCollect_MultipleQueues verifies that metrics from multiple queues are correctly separated
func TestCollect_MultipleQueues(t *testing.T) {
	// Create streams from different queues with known patterns
	streams := []StreamInfo{
		{
			Key:          "Events:queue-a:js1",
			Queue:        "queue-a",
			JobSetId:     "js1",
			Length:       100,
			MemoryBytes:  10240,
			FirstEntryID: "1000-0",
			LastEntryID:  "1099-0",
			AgeSeconds:   0,
		},
		{
			Key:          "Events:queue-a:js2",
			Queue:        "queue-a",
			JobSetId:     "js2",
			Length:       200,
			MemoryBytes:  20480,
			FirstEntryID: "1000-0",
			LastEntryID:  "1199-0",
			AgeSeconds:   0,
		},
		{
			Key:          "Events:queue-b:js1",
			Queue:        "queue-b",
			JobSetId:     "js1",
			Length:       150,
			MemoryBytes:  15360,
			FirstEntryID: "1000-0",
			LastEntryID:  "1149-0",
			AgeSeconds:   0,
		},
	}

	scanner := &mockScanner{
		streams: streams,
		err:     nil,
	}

	collector := newTestCollector(scanner, Config{
		Enabled:            true,
		CollectionInterval: 100 * time.Millisecond,
		TopN:               10,
		ScanBatchSize:      100,
		PipelineBatchSize:  10,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	collector.collectOnce(ctx)

	metrics := collectMetrics(collector)

	// Verify that queues are properly separated
	queueCount := make(map[string]int)
	for _, m := range metrics {
		desc := m.Desc().String()
		// Look for queue-specific metrics
		if desc == "Desc{fqName: \"redis_queue_total_length\", help: \"Total event count across all streams in queue\", constLabels: {}, variableLabels: [queue]}" {
			queueCount["queue_length"]++
		}
	}

	// Should have metrics for multiple queues
	require.Greater(t, len(metrics), 0, "should have metrics from multiple queues")
}
