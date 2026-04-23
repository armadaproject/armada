package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/repository"
	"github.com/armadaproject/armada/internal/scheduler/leader"
)

func testCollectorConfig(topN int) configuration.RedisMemoryMetricsConfig {
	return configuration.RedisMemoryMetricsConfig{
		CollectionInterval:        100 * time.Millisecond,
		InitialCollectionDelayMax: 1 * time.Millisecond,
		TopN:                      topN,
		ScanBatchSize:             100,
		PipelineBatchSize:         10,
		InterBatchDelay:           0,
		MemoryUsageSamples:        5,
	}
}

func newRedisBackedCollector(client redis.UniversalClient, config configuration.RedisMemoryMetricsConfig, leaderController leader.LeaderController) *Collector {
	return NewCollector(repository.NewScanner(client, config), config, leaderController)
}

func seedGeneratedStreams(t *testing.T, client redis.UniversalClient, ctx context.Context, count int, queuePrefix string) {
	t.Helper()

	for i := range count {
		queue := fmt.Sprintf("%s-%03d", queuePrefix, i)
		jobSetId := fmt.Sprintf("jobset-%03d", i)
		seedRedisStream(t, client, ctx, queue, jobSetId, (i+1)*10)
	}
}

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

func gaugeMetricValues(t *testing.T, collector prometheus.Collector) map[string]float64 {
	t.Helper()

	ch := make(chan prometheus.Metric, 1000)
	collector.Collect(ch)
	close(ch)

	metrics := make(map[string]float64)
	for metric := range ch {
		pb := &dto.Metric{}
		require.NoError(t, metric.Write(pb))

		labels := make([]string, 0, len(pb.Label))
		for _, label := range pb.Label {
			labels = append(labels, fmt.Sprintf("%s=%s", label.GetName(), label.GetValue()))
		}
		sort.Strings(labels)

		if pb.Gauge != nil {
			metrics[strings.Join(labels, ",")] = pb.GetGauge().GetValue()
		}
	}

	return metrics
}

func topNLabelSet(streams []repository.StreamInfo, topN int, less func(a, b repository.StreamInfo) bool) map[string]struct{} {
	sorted := make([]repository.StreamInfo, len(streams))
	copy(sorted, streams)
	sort.Slice(sorted, func(i, j int) bool {
		return less(sorted[i], sorted[j])
	})

	if topN > len(sorted) {
		topN = len(sorted)
	}

	result := make(map[string]struct{}, topN)
	for i := 0; i < topN; i++ {
		result[fmt.Sprintf("jobset=%s,queue=%s", sorted[i].JobSetId, sorted[i].Queue)] = struct{}{}
	}

	return result
}

func metricKeys(metrics map[string]float64) map[string]struct{} {
	result := make(map[string]struct{}, len(metrics))
	for key := range metrics {
		result[key] = struct{}{}
	}
	return result
}

func TestCollect_EmptyStreams(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		collector := newRedisBackedCollector(client, testCollectorConfig(5), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		require.Zero(t, testutil.CollectAndCount(collector.topNMemoryGauge))
		require.Zero(t, testutil.CollectAndCount(collector.topNEventsGauge))
		require.Zero(t, testutil.CollectAndCount(collector.queueStreamsGauge))
	})
}

func TestCollect_TopN_Memory(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 30, "queue-memory")

		config := testCollectorConfig(5)
		scanner := repository.NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)

		collector := NewCollector(scanner, config, leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		expected := topNLabelSet(streams, config.TopN, func(a, b repository.StreamInfo) bool {
			return a.MemoryBytes > b.MemoryBytes
		})
		actual := metricKeys(gaugeMetricValues(t, collector.topNMemoryGauge))

		require.Equal(t, expected, actual)
	})
}

func TestCollect_TopN_Events(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 30, "queue-events")

		config := testCollectorConfig(5)
		scanner := repository.NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)

		collector := NewCollector(scanner, config, leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		expected := topNLabelSet(streams, config.TopN, func(a, b repository.StreamInfo) bool {
			return a.Length > b.Length
		})
		actual := metricKeys(gaugeMetricValues(t, collector.topNEventsGauge))

		require.Equal(t, expected, actual)
	})
}

func TestCollect_PerQueueAggregation(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		for i := 1; i <= 5; i++ {
			seedRedisStream(t, client, ctx, "queue1", fmt.Sprintf("jobset-%d", i), i*10)
			seedRedisStream(t, client, ctx, "queue2", fmt.Sprintf("jobset-%d", i), i*10)
			seedRedisStream(t, client, ctx, "queue3", fmt.Sprintf("jobset-%d", i), i*10)
		}

		collector := newRedisBackedCollector(client, testCollectorConfig(20), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		require.Equal(t, 5.0, testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue1")))
		require.Equal(t, 5.0, testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue2")))
		require.Equal(t, 5.0, testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue3")))
		require.Equal(t, 150.0, testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue1")))
		require.Equal(t, 150.0, testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue2")))
		require.Equal(t, 150.0, testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue3")))
	})
}

func TestCollect_StaleLabelsCleared(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		for i := range 10 {
			seedRedisStream(t, client, ctx, "queue-stale-a", fmt.Sprintf("old-%d", i), (i+1)*10)
		}

		config := testCollectorConfig(5)
		collector := newRedisBackedCollector(client, config, leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		firstMetrics := metricKeys(gaugeMetricValues(t, collector.topNMemoryGauge))
		require.Len(t, firstMetrics, 5)

		require.NoError(t, client.FlushDB(ctx).Err())
		for i := range 5 {
			seedRedisStream(t, client, ctx, "queue-stale-b", fmt.Sprintf("new-%d", i), (i+1)*50)
		}

		require.NoError(t, collector.collectOnce(ctx))

		secondMetrics := metricKeys(gaugeMetricValues(t, collector.topNMemoryGauge))
		expectedSecond := map[string]struct{}{}
		for i := range 5 {
			expectedSecond[fmt.Sprintf("jobset=new-%d,queue=queue-stale-b", i)] = struct{}{}
		}

		require.Equal(t, expectedSecond, secondMetrics)
		for key := range firstMetrics {
			_, stillPresent := secondMetrics[key]
			require.False(t, stillPresent)
		}
	})
}

func TestCollect_ScannerError(t *testing.T) {
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		collector := newRedisBackedCollector(client, testCollectorConfig(5), leader.NewStandaloneLeaderController())

		err := collector.collectOnce(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "scanner error: context canceled")
		require.Equal(t, 1.0, testutil.ToFloat64(collector.errorsTotal))
	})
}

func TestCollect_SkipIfBusy(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 40, "queue-busy")

		config := testCollectorConfig(5)
		config.ScanBatchSize = 5
		config.PipelineBatchSize = 2
		config.InterBatchDelay = 100 * time.Millisecond

		collector := newRedisBackedCollector(client, config, leader.NewStandaloneLeaderController())

		firstErr := make(chan error, 1)
		go func() {
			firstErr <- collector.collectOnce(ctx)
		}()

		time.Sleep(20 * time.Millisecond)
		err := collector.collectOnce(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "previous collection still running, skipping")
		require.NoError(t, <-firstErr)
	})
}

func TestCollect_MetricDescriptions(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedRedisStream(t, client, ctx, "queue-desc", "jobset-1", 10)

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		descCh := make(chan *prometheus.Desc, 100)
		go func() {
			collector.Describe(descCh)
			close(descCh)
		}()

		descCount := 0
		for range descCh {
			descCount++
		}

		require.Equal(t, 13, descCount)
	})
}

func TestCollect_ConcurrentCollect(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 20, "queue-concurrent")

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				metrics := collectMetrics(collector)
				require.Greater(t, len(metrics), 0)
			})
		}
		wg.Wait()
	})
}

func TestCollect_ContextCancellation(t *testing.T) {
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedCtx, seedCancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
		defer seedCancel()
		seedGeneratedStreams(t, client, seedCtx, 20, "queue-cancel")

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leader.NewStandaloneLeaderController())

		err := collector.collectOnce(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "scanner error: context canceled")
		require.NotNil(t, collectMetrics(collector))
	})
}

func TestCollect_LargeDataset(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 20, "queue-large")

		collector := newRedisBackedCollector(client, testCollectorConfig(10), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))
		require.Equal(t, 10, testutil.CollectAndCount(collector.topNMemoryGauge))
		require.Greater(t, testutil.ToFloat64(collector.streamsScannedGauge), 0.0)
	})
}

func TestCollect_NoMetricsWhenDisabled(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedRedisStream(t, client, ctx, "queue-disabled", "jobset-1", 10)

		config := testCollectorConfig(5)
		collector := newRedisBackedCollector(client, config, leader.NewStandaloneLeaderController())

		require.Len(t, collectMetrics(collector), 0)
	})
}

func TestCollect_MultipleQueues(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedRedisStream(t, client, ctx, "queue-a", "js1", 100)
		seedRedisStream(t, client, ctx, "queue-a", "js2", 200)
		seedRedisStream(t, client, ctx, "queue-b", "js1", 150)

		collector := newRedisBackedCollector(client, testCollectorConfig(10), leader.NewStandaloneLeaderController())
		require.NoError(t, collector.collectOnce(ctx))

		require.Equal(t, 2.0, testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue-a")))
		require.Equal(t, 1.0, testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue-b")))
		require.Equal(t, 300.0, testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue-a")))
		require.Equal(t, 150.0, testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue-b")))
	})
}

func TestCollector_LeaderMode_EmitsMetrics(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 10, "queue-leader")

		leaderController := leader.NewStandaloneLeaderController()
		leaderController.SetToken(leader.NewLeaderToken())

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)
		require.NoError(t, collector.collectOnce(ctx))

		metrics := collectMetrics(collector)
		require.Greater(t, len(metrics), 0)

		hasRedisMetrics := false
		for _, m := range metrics {
			desc := m.Desc().String()
			if strings.Contains(desc, ArmadaRedisMetricsPrefix+"queue") || strings.Contains(desc, ArmadaRedisMetricsPrefix+"stream") {
				hasRedisMetrics = true
				break
			}
		}

		require.True(t, hasRedisMetrics)
	})
}

func TestCollector_NonLeaderMode_NoMetrics(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 10, "queue-nonleader")

		leaderController := leader.NewStandaloneLeaderController()
		leaderController.SetToken(leader.NewLeaderToken())

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)
		require.NoError(t, collector.collectOnce(ctx))

		leaderController.SetToken(leader.InvalidLeaderToken())
		collector.ClearState()

		for _, m := range collectMetrics(collector) {
			require.False(t, strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix))
		}
	})
}

func TestCollector_LeadershipTransition(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 10, "queue-transition")

		leaderController := leader.NewStandaloneLeaderController()
		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)

		leaderController.SetToken(leader.NewLeaderToken())
		require.NoError(t, collector.collectOnce(ctx))
		metricsAsLeader := collectMetrics(collector)
		require.Greater(t, len(metricsAsLeader), 0)

		hasQueueMetricsAsLeader := false
		for _, m := range metricsAsLeader {
			if strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"queue") || strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"stream") {
				hasQueueMetricsAsLeader = true
				break
			}
		}
		require.True(t, hasQueueMetricsAsLeader)

		leaderController.SetToken(leader.InvalidLeaderToken())
		collector.ClearState()
		for _, m := range collectMetrics(collector) {
			require.False(t, strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"queue"))
			require.False(t, strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"stream"))
		}

		leaderController.SetToken(leader.NewLeaderToken())
		require.NoError(t, collector.collectOnce(ctx))
		metricsAfterRegaining := collectMetrics(collector)

		hasQueueMetricsAfterRegain := false
		for _, m := range metricsAfterRegaining {
			if strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"queue") || strings.Contains(m.Desc().String(), ArmadaRedisMetricsPrefix+"stream") {
				hasQueueMetricsAfterRegain = true
				break
			}
		}

		require.True(t, hasQueueMetricsAfterRegain)
	})
}

func TestCollector_Describe_LeadershipIndependent(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		leaderController := leader.NewStandaloneLeaderController()
		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)

		leaderController.SetToken(leader.NewLeaderToken())
		ch1 := make(chan *prometheus.Desc, 100)
		collector.Describe(ch1)
		close(ch1)

		var descriptorsAsLeader []*prometheus.Desc
		for desc := range ch1 {
			descriptorsAsLeader = append(descriptorsAsLeader, desc)
		}

		leaderController.SetToken(leader.InvalidLeaderToken())
		ch2 := make(chan *prometheus.Desc, 100)
		collector.Describe(ch2)
		close(ch2)

		var descriptorsAsNonLeader []*prometheus.Desc
		for desc := range ch2 {
			descriptorsAsNonLeader = append(descriptorsAsNonLeader, desc)
		}

		require.Equal(t, len(descriptorsAsLeader), len(descriptorsAsNonLeader))

		leaderDescStrings := make(map[string]bool, len(descriptorsAsLeader))
		for _, desc := range descriptorsAsLeader {
			leaderDescStrings[desc.String()] = true
		}

		for _, desc := range descriptorsAsNonLeader {
			require.True(t, leaderDescStrings[desc.String()])
		}
	})
}

func TestCollector_ScrapetimeLeadershipCheck(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 10, "queue-scrapetime")

		leaderController := leader.NewStandaloneLeaderController()
		leaderController.SetToken(leader.NewLeaderToken())

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)
		require.NoError(t, collector.collectOnce(ctx))

		metricsAsLeader := collectMetrics(collector)
		require.Greater(t, len(metricsAsLeader), 0)

		leaderController.SetToken(leader.InvalidLeaderToken())
		metricsAfterLeadershipLoss := collectMetrics(collector)
		require.Len(t, metricsAfterLeadershipLoss, 0)

		for range 2 {
			require.Len(t, collectMetrics(collector), 0)
		}
	})
}

func TestCollector_ScrapetimeLeadershipCheck_RunsAfterRegain(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		seedGeneratedStreams(t, client, ctx, 10, "queue-scrapetime-regain")

		leaderController := leader.NewStandaloneLeaderController()
		leaderController.SetToken(leader.NewLeaderToken())

		collector := newRedisBackedCollector(client, testCollectorConfig(5), leaderController)
		require.NoError(t, collector.collectOnce(ctx))
		require.Greater(t, len(collectMetrics(collector)), 0)

		leaderController.SetToken(leader.InvalidLeaderToken())
		require.Len(t, collectMetrics(collector), 0)

		leaderController.SetToken(leader.NewLeaderToken())
		require.Greater(t, len(collectMetrics(collector)), 0)
	})
}

type mockScanner struct {
	streams []repository.StreamInfo
	err     error
}

func (m *mockScanner) ScanAll(ctx context.Context) ([]repository.StreamInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.streams, nil
}

type blockingMockScanner struct {
	streams   []repository.StreamInfo
	err       error
	startedCh chan int
	unblockCh chan struct{}
	calls     atomic.Int64
}

func newBlockingMockScanner(streams []repository.StreamInfo) *blockingMockScanner {
	return &blockingMockScanner{
		streams:   streams,
		startedCh: make(chan int, 10),
		unblockCh: make(chan struct{}, 10),
	}
}

func (m *blockingMockScanner) ScanAll(ctx context.Context) ([]repository.StreamInfo, error) {
	call := int(m.calls.Add(1))
	select {
	case m.startedCh <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-m.unblockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if m.err != nil {
		return nil, m.err
	}

	return m.streams, nil
}

func waitForScanCall(t *testing.T, startedCh <-chan int, expectedCall int) {
	t.Helper()

	select {
	case got := <-startedCh:
		require.Equal(t, expectedCall, got)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for scan call %d", expectedCall)
	}
}

func assertNoScanCall(t *testing.T, startedCh <-chan int, wait time.Duration) {
	t.Helper()

	select {
	case got := <-startedCh:
		t.Fatalf("unexpected scan call %d", got)
	case <-time.After(wait):
	}
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()

	pb := &dto.Metric{}
	require.NoError(t, h.Write(pb))
	require.NotNil(t, pb.Histogram)

	return pb.Histogram.GetSampleCount()
}

func TestCollector_ResetMetricsForNewCycle_ResetsSampleCounts(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	scanner := &mockScanner{
		streams: []repository.StreamInfo{
			{Queue: "q1", JobSetId: "js1", MemoryBytes: 1024, Length: 10, AgeSeconds: 60},
			{Queue: "q2", JobSetId: "js2", MemoryBytes: 2048, Length: 20, AgeSeconds: 120},
			{Queue: "q3", JobSetId: "js3", MemoryBytes: 4096, Length: 30, AgeSeconds: 240},
		},
	}

	collector := NewCollector(scanner, testCollectorConfig(5), leader.NewStandaloneLeaderController())
	require.NoError(t, collector.collectOnce(ctx))

	require.Equal(t, uint64(3), histogramSampleCount(t, collector.bytesHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.eventsHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.ageHistogram))

	collector.resetMetricsForNewCycle()

	require.Zero(t, histogramSampleCount(t, collector.bytesHistogram))
	require.Zero(t, histogramSampleCount(t, collector.eventsHistogram))
	require.Zero(t, histogramSampleCount(t, collector.ageHistogram))

	require.NoError(t, collector.collectOnce(ctx))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.bytesHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.eventsHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.ageHistogram))
}

func TestCollector_ResetMetricsForNewCycle_ResetsGaugesButNotCounters(t *testing.T) {
	collector := NewCollector(&mockScanner{}, testCollectorConfig(5), leader.NewStandaloneLeaderController())

	collector.queueStreamsGauge.WithLabelValues("queue-a").Set(5)
	collector.queueMemoryGauge.WithLabelValues("queue-a").Set(2048)
	collector.queueEventsGauge.WithLabelValues("queue-a").Set(150)
	collector.errorsTotal.Inc()
	collector.errorsTotal.Inc()

	collector.resetMetricsForNewCycle()

	require.Equal(t, float64(0), testutil.ToFloat64(collector.queueStreamsGauge.WithLabelValues("queue-a")))
	require.Equal(t, float64(0), testutil.ToFloat64(collector.queueMemoryGauge.WithLabelValues("queue-a")))
	require.Equal(t, float64(0), testutil.ToFloat64(collector.queueEventsGauge.WithLabelValues("queue-a")))
	require.Equal(t, float64(2), testutil.ToFloat64(collector.errorsTotal))
}

func TestCollector_Run_ContinuousLeadership_ResetsHistogramsBeforeEachCollect(t *testing.T) {
	streams := []repository.StreamInfo{{Queue: "q1", JobSetId: "js1", MemoryBytes: 1024, Length: 10, AgeSeconds: 60}}
	scanner := newBlockingMockScanner(streams)

	config := testCollectorConfig(5)
	config.CollectionInterval = 200 * time.Millisecond

	leaderController := leader.NewStandaloneLeaderController()
	leaderController.SetToken(leader.NewLeaderToken())

	collector := NewCollector(scanner, config, leaderController)

	runCtx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- collector.Run(runCtx)
	}()

	waitForScanCall(t, scanner.startedCh, 1)
	scanner.unblockCh <- struct{}{}
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	waitForScanCall(t, scanner.startedCh, 2)
	scanner.unblockCh <- struct{}{}

	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-runErr)
}

func TestCollector_Run_MultipleLeadershipFlaps_ReallocatesOnEachReacquisition(t *testing.T) {
	streams := []repository.StreamInfo{{Queue: "q1", JobSetId: "js1", MemoryBytes: 1024, Length: 10, AgeSeconds: 60}}
	scanner := newBlockingMockScanner(streams)

	config := testCollectorConfig(5)
	config.CollectionInterval = 200 * time.Millisecond

	leaderController := leader.NewStandaloneLeaderController()
	leaderController.SetToken(leader.NewLeaderToken())

	collector := NewCollector(scanner, config, leaderController)

	runCtx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- collector.Run(runCtx)
	}()

	waitForScanCall(t, scanner.startedCh, 1)
	scanner.unblockCh <- struct{}{}
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	leaderController.SetToken(leader.InvalidLeaderToken())
	time.Sleep(config.CollectionInterval + 50*time.Millisecond)
	leaderController.SetToken(leader.NewLeaderToken())

	waitForScanCall(t, scanner.startedCh, 2)
	scanner.unblockCh <- struct{}{}
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	leaderController.SetToken(leader.InvalidLeaderToken())
	time.Sleep(config.CollectionInterval + 50*time.Millisecond)
	leaderController.SetToken(leader.NewLeaderToken())

	waitForScanCall(t, scanner.startedCh, 3)
	scanner.unblockCh <- struct{}{}
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-runErr)
}

func TestCollector_Run_StartsNonLeader_ThenBecomesLeader_ReallocatesBeforeFirstCollect(t *testing.T) {
	streams := []repository.StreamInfo{{Queue: "q1", JobSetId: "js1", MemoryBytes: 1024, Length: 10, AgeSeconds: 60}}
	scanner := newBlockingMockScanner(streams)

	config := testCollectorConfig(5)
	config.CollectionInterval = 200 * time.Millisecond

	leaderController := leader.NewStandaloneLeaderController()
	leaderController.SetToken(leader.InvalidLeaderToken())

	collector := NewCollector(scanner, config, leaderController)

	runCtx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- collector.Run(runCtx)
	}()

	assertNoScanCall(t, scanner.startedCh, config.CollectionInterval+50*time.Millisecond)

	leaderController.SetToken(leader.NewLeaderToken())
	waitForScanCall(t, scanner.startedCh, 1)
	scanner.unblockCh <- struct{}{}
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, collector.bytesHistogram) == 1
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	require.NoError(t, <-runErr)
}

func TestCollector_CollectOnce_ResetsHistogramsEveryCycle(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	scanner := &mockScanner{
		streams: []repository.StreamInfo{
			{Queue: "q1", JobSetId: "js1", MemoryBytes: 1024, Length: 10, AgeSeconds: 60},
			{Queue: "q2", JobSetId: "js2", MemoryBytes: 2048, Length: 20, AgeSeconds: 120},
			{Queue: "q3", JobSetId: "js3", MemoryBytes: 4096, Length: 30, AgeSeconds: 240},
		},
	}

	collector := NewCollector(scanner, testCollectorConfig(5), leader.NewStandaloneLeaderController())
	require.NoError(t, collector.collectOnce(ctx))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.bytesHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.eventsHistogram))
	require.Equal(t, uint64(3), histogramSampleCount(t, collector.ageHistogram))

	scanner.streams = []repository.StreamInfo{{Queue: "q4", JobSetId: "js4", MemoryBytes: 8192, Length: 40, AgeSeconds: 480}}

	require.NoError(t, collector.collectOnce(ctx))
	require.Equal(t, uint64(1), histogramSampleCount(t, collector.bytesHistogram))
	require.Equal(t, uint64(1), histogramSampleCount(t, collector.eventsHistogram))
	require.Equal(t, uint64(1), histogramSampleCount(t, collector.ageHistogram))
}

func TestHistogramBuckets_BytesDistribution(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name          string
		memoryBytes   int64
		expectedCount uint64
	}{
		{"small_1KB", 1024, 1},
		{"medium_1MB", 1024 * 1024, 1},
		{"large_100MB", 100 * 1024 * 1024, 1},
		{"very_large_1GB", 1024 * 1024 * 1024, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &mockScanner{
				streams: []repository.StreamInfo{
					{
						Queue:       "test-queue",
						JobSetId:    "test-jobset",
						Key:         constants.EventStreamPrefix + "test-queue:test-jobset",
						MemoryBytes: tc.memoryBytes,
						Length:      100,
						AgeSeconds:  300,
					},
				},
			}

			collector := NewCollector(scanner, testCollectorConfig(5), leader.NewStandaloneLeaderController())
			require.NoError(t, collector.collectOnce(ctx))

			pb := &dto.Metric{}
			require.NoError(t, collector.bytesHistogram.Write(pb))
			require.NotNil(t, pb.Histogram)
			require.Equal(t, tc.expectedCount, pb.Histogram.GetSampleCount())
		})
	}
}

func TestHistogramBuckets_EventsDistribution(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name          string
		eventCount    int64
		expectedCount uint64
	}{
		{"tiny_10_events", 10, 1},
		{"small_1K_events", 1000, 1},
		{"medium_100K_events", 100000, 1},
		{"large_1M_events", 1000000, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &mockScanner{
				streams: []repository.StreamInfo{
					{
						Queue:       "test-queue",
						JobSetId:    "test-jobset",
						Key:         constants.EventStreamPrefix + "test-queue:test-jobset",
						MemoryBytes: 10240,
						Length:      tc.eventCount,
						AgeSeconds:  300,
					},
				},
			}

			collector := NewCollector(scanner, testCollectorConfig(5), leader.NewStandaloneLeaderController())
			require.NoError(t, collector.collectOnce(ctx))

			pb := &dto.Metric{}
			require.NoError(t, collector.eventsHistogram.Write(pb))
			require.NotNil(t, pb.Histogram)
			require.Equal(t, tc.expectedCount, pb.Histogram.GetSampleCount())
		})
	}
}

func TestHistogramBuckets_AgeDistribution(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	testCases := []struct {
		name          string
		ageSeconds    float64
		expectedCount uint64
	}{
		{"fresh_1min", 60, 1},
		{"recent_1hour", 3600, 1},
		{"old_1day", 86400, 1},
		{"very_old_7days", 604800, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &mockScanner{
				streams: []repository.StreamInfo{
					{
						Queue:       "test-queue",
						JobSetId:    "test-jobset",
						Key:         constants.EventStreamPrefix + "test-queue:test-jobset",
						MemoryBytes: 10240,
						Length:      100,
						AgeSeconds:  tc.ageSeconds,
					},
				},
			}

			collector := NewCollector(scanner, testCollectorConfig(5), leader.NewStandaloneLeaderController())
			require.NoError(t, collector.collectOnce(ctx))

			pb := &dto.Metric{}
			require.NoError(t, collector.ageHistogram.Write(pb))
			require.NotNil(t, pb.Histogram)
			require.Equal(t, tc.expectedCount, pb.Histogram.GetSampleCount())
		})
	}
}

func withRedisClient(ctx *armadacontext.Context, action func(client redis.UniversalClient)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 7})
	defer client.FlushDB(ctx)
	defer client.Close()

	client.FlushDB(ctx)
	action(client)
}

func seedRedisStream(t *testing.T, client redis.UniversalClient, ctx context.Context, queue, jobSetId string, entryCount int) string {
	t.Helper()

	streamKey := fmt.Sprintf("%s%s:%s", constants.EventStreamPrefix, queue, jobSetId)

	for i := range entryCount {
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{"index": i},
		}).Result()
		require.NoError(t, err)
	}
	return streamKey
}
