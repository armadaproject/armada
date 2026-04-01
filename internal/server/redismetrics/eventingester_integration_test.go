package redismetrics

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/scheduler/leader"
)

func seedRedisStreams(t *testing.T, client redis.UniversalClient, ctx context.Context) {
	t.Helper()

	streams := []struct {
		queue    string
		jobSetId string
		count    int
	}{
		{"test-queue-1", "jobset-a", 100},
		{"test-queue-1", "jobset-b", 50},
		{"test-queue-2", "jobset-a", 200},
		{"test-queue-2", "jobset-b", 75},
		{"test-queue-3", "jobset-a", 10},
	}

	for _, s := range streams {
		streamKey := fmt.Sprintf("Events:%s:%s", s.queue, s.jobSetId)
		for i := 0; i < s.count; i++ {
			_, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamKey,
				Values: map[string]interface{}{"index": i, "data": fmt.Sprintf("event-%d", i)},
			}).Result()
			require.NoError(t, err)
		}
	}
}

func TestEventIngester_RedisMetrics_PublicationUnderLoad(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
		defer cancel()

		seedRedisStreams(t, client, ctx)

		config := Config{
			CollectionInterval: 5 * time.Minute,
			TopN:               5,
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}

		scanner := NewScanner(client, config)

		leaderController := leader.NewStandaloneLeaderController()
		leaderController.SetToken(leader.NewLeaderToken())

		collector := NewCollector(scanner, config, leaderController)
		registry := prometheus.NewRegistry()
		registry.MustRegister(collector)

		err := collector.collectOnce(ctx)
		require.NoError(t, err)

		metricFamilies, err := registry.Gather()
		require.NoError(t, err)

		foundMetrics := make(map[string]bool)
		metricValues := make(map[string]float64)

		for _, mf := range metricFamilies {
			name := mf.GetName()
			if strings.HasPrefix(name, "armada_event_redis_") {
				foundMetrics[name] = true

				for _, m := range mf.GetMetric() {
					switch {
					case m.GetGauge() != nil:
						metricValues[name] = m.GetGauge().GetValue()
					case m.GetCounter() != nil:
						metricValues[name] = m.GetCounter().GetValue()
					case m.GetHistogram() != nil:
						metricValues[name] = m.GetHistogram().GetSampleSum()
					}
				}
			}
		}

		expectedMetrics := []string{
			"armada_event_redis_stream_memory_bytes",
			"armada_event_redis_stream_event_count",
			"armada_event_redis_stream_age_seconds",
			"armada_event_redis_queue_streams_total",
			"armada_event_redis_queue_events_total",
			"armada_event_redis_queue_memory_bytes_total",
			"armada_event_redis_stream_size_bytes_distribution",
			"armada_event_redis_stream_size_events_distribution",
			"armada_event_redis_stream_age_seconds_distribution",
		}

		for _, expected := range expectedMetrics {
			assert.True(t, foundMetrics[expected], "Expected metric family %s not found", expected)
		}

		assert.Greater(t, metricValues["armada_event_redis_stream_event_count"], float64(0),
			"Expected non-zero stream event count metric")
		assert.Greater(t, metricValues["armada_event_redis_queue_events_total"], float64(0),
			"Expected non-zero queue event count")

		var foundQueueLabel bool
		var foundJobsetLabel bool
		for _, mf := range metricFamilies {
			if strings.HasPrefix(mf.GetName(), "armada_event_redis_stream_") {
				for _, m := range mf.GetMetric() {
					for _, label := range m.GetLabel() {
						if label.GetName() == "queue" {
							foundQueueLabel = true
						}
						if label.GetName() == "jobset" {
							foundJobsetLabel = true
						}
					}
				}
			}
		}
		assert.True(t, foundQueueLabel, "Expected 'queue' label in stream metrics")
		assert.True(t, foundJobsetLabel, "Expected 'jobset' label in stream metrics")
	})
}

func TestEventIngester_RedisMetrics_GracefulFailureWhenRedisUnavailable(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	unavailableClient := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:9999",
		DB:           testRedisDB,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
	})
	defer unavailableClient.Close()

	config := Config{
		CollectionInterval: 5 * time.Minute,
		TopN:               5,
		ScanBatchSize:      1000,
		PipelineBatchSize:  500,
		InterBatchDelay:    0,
		MemoryUsageSamples: 5,
	}

	scanner := NewScanner(unavailableClient, config)

	leaderController := leader.NewStandaloneLeaderController()
	leaderController.SetToken(leader.NewLeaderToken())

	collector := NewCollector(scanner, config, leaderController)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	err := collector.collectOnce(ctx)
	assert.Error(t, err, "Expected error when Redis is unavailable")
	assert.Contains(t, err.Error(), "connection refused", "Expected connection refused error")

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	dataMetricCount := 0
	for _, mf := range metricFamilies {
		name := mf.GetName()
		if !strings.HasPrefix(name, "armada_event_redis_") {
			continue
		}
		if strings.Contains(name, "metrics_errors_total") ||
			strings.Contains(name, "metrics_last_collection_timestamp") ||
			strings.Contains(name, "metrics_collection_duration_seconds") ||
			strings.Contains(name, "metrics_streams_scanned_total") {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasNonZeroValue(m) {
				dataMetricCount++
			}
		}
	}

	assert.Equal(t, 0, dataMetricCount,
		"Expected no data metrics with non-zero values when collection fails")
}

func hasNonZeroValue(m *dto.Metric) bool {
	if g := m.GetGauge(); g != nil && g.GetValue() != 0 {
		return true
	}
	if c := m.GetCounter(); c != nil && c.GetValue() != 0 {
		return true
	}
	if h := m.GetHistogram(); h != nil && h.GetSampleCount() > 0 {
		return true
	}
	return false
}

func TestEventIngester_RedisMetrics_LeaderElection(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
		defer cancel()

		seedRedisStreams(t, client, ctx)

		config := Config{
			CollectionInterval: 5 * time.Minute,
			TopN:               5,
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}

		scanner := NewScanner(client, config)
		leaderController := leader.NewStandaloneLeaderController()

		collector := NewCollector(scanner, config, leaderController)
		registry := prometheus.NewRegistry()
		registry.MustRegister(collector)

		leaderController.SetToken(leader.InvalidLeaderToken())
		err := collector.collectOnce(ctx)
		require.NoError(t, err)

		metricFamilies, err := registry.Gather()
		require.NoError(t, err)

		for _, mf := range metricFamilies {
			if strings.HasPrefix(mf.GetName(), "armada_event_redis_") {
				for _, m := range mf.GetMetric() {
					assert.False(t, hasNonZeroValue(m),
						"Non-leader should not emit redis metrics: %s", mf.GetName())
				}
			}
		}

		leaderController.SetToken(leader.NewLeaderToken())
		err = collector.collectOnce(ctx)
		require.NoError(t, err)

		metricFamilies, err = registry.Gather()
		require.NoError(t, err)

		foundLeaderMetrics := false
		for _, mf := range metricFamilies {
			if strings.HasPrefix(mf.GetName(), "armada_event_redis_") {
				for _, m := range mf.GetMetric() {
					if hasNonZeroValue(m) {
						foundLeaderMetrics = true
						break
					}
				}
			}
		}
		assert.True(t, foundLeaderMetrics, "Leader should emit redis metrics")
	})
}

func TestEventIngester_RedisMetrics_ConfigValidation(t *testing.T) {
	cfg := &configuration.EventIngesterConfiguration{
		Metrics: configuration.MetricsConfig{
			Redis: configuration.RedisMemoryMetricsConfig{
				Enabled:            true,
				CollectionInterval: 1 * time.Minute,
				TopN:               10,
				ScanBatchSize:      500,
				PipelineBatchSize:  250,
				InterBatchDelay:    100 * time.Millisecond,
				MemoryUsageSamples: 10,
				Leader: configuration.LeaderConfig{
					Mode: "standalone",
				},
			},
		},
	}

	assert.True(t, cfg.Metrics.Redis.Enabled)
	assert.Equal(t, 1*time.Minute, cfg.Metrics.Redis.CollectionInterval)
	assert.Equal(t, 10, cfg.Metrics.Redis.TopN)
	assert.Equal(t, int64(500), cfg.Metrics.Redis.ScanBatchSize)
	assert.Equal(t, 250, cfg.Metrics.Redis.PipelineBatchSize)
	assert.Equal(t, 100*time.Millisecond, cfg.Metrics.Redis.InterBatchDelay)
	assert.Equal(t, 10, cfg.Metrics.Redis.MemoryUsageSamples)
	assert.Equal(t, "standalone", cfg.Metrics.Redis.Leader.Mode)
}
