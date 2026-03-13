package redismetrics

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func createTestStream(t *testing.T, client redis.UniversalClient, ctx context.Context, queue, jobSetId string, eventCount int) string {
	streamKey := fmt.Sprintf("Events:%s:%s", queue, jobSetId)

	compressor, err := compress.NewZlibCompressor(0)
	require.NoError(t, err)

	for i := 0; i < eventCount; i++ {
		es := &armadaevents.EventSequence{
			Queue:      queue,
			JobSetName: jobSetId,
			Events: []*armadaevents.EventSequence_Event{{
				Event: &armadaevents.EventSequence_Event_JobRunRunning{
					JobRunRunning: &armadaevents.JobRunRunning{
						RunId: fmt.Sprintf("00000000-0000-0000-0000-%012d", i),
						JobId: fmt.Sprintf("01f3j0g1md4qx7z5qb148q%04d", i),
					},
				},
			}},
		}

		bytes, err := proto.Marshal(es)
		require.NoError(t, err)

		compressed, err := compressor.Compress(bytes)
		require.NoError(t, err)

		_, err = client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{"message": compressed},
		}).Result()
		require.NoError(t, err)
	}
	return streamKey
}

func TestIntegration_ScanAll_RealRedis(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		createTestStream(t, client, ctx, "queue1", "jobset1", 10)
		createTestStream(t, client, ctx, "queue1", "jobset2", 20)
		createTestStream(t, client, ctx, "queue2", "jobset1", 30)
		createTestStream(t, client, ctx, "queue2", "jobset2", 40)
		createTestStream(t, client, ctx, "queue3", "jobset1", 50)

		config := Config{
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}
		scanner := NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 5)

		streamMap := make(map[string]StreamInfo)
		for _, s := range streams {
			streamMap[s.Key] = s
		}

		s1, ok := streamMap["Events:queue1:jobset1"]
		require.True(t, ok)
		assert.Equal(t, "queue1", s1.Queue)
		assert.Equal(t, "jobset1", s1.JobSetId)
		assert.Equal(t, int64(10), s1.Length)

		s2, ok := streamMap["Events:queue2:jobset2"]
		require.True(t, ok)
		assert.Equal(t, "queue2", s2.Queue)
		assert.Equal(t, "jobset2", s2.JobSetId)
		assert.Equal(t, int64(40), s2.Length)
	})
}

func TestIntegration_ScanAll_MemoryUsage(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		createTestStream(t, client, ctx, "queue-memory", "jobset-memory", 100)

		config := Config{
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}
		scanner := NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 1)

		stream := streams[0]
		assert.Greater(t, stream.MemoryBytes, int64(0))
		assert.Greater(t, stream.MemoryBytes, int64(1024))
	})
}

func TestIntegration_ScanAll_AgeComputation(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		streamKey := createTestStream(t, client, ctx, "queue-age", "jobset-age", 5)
		time.Sleep(2 * time.Second)

		config := Config{
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}
		scanner := NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 1)

		stream := streams[0]
		assert.Equal(t, streamKey, stream.Key)
		assert.InDelta(t, 2.0, stream.AgeSeconds, 2.0)
		assert.Greater(t, stream.AgeSeconds, 0.0)
	})
}

func TestIntegration_CollectorMetrics(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
		defer cancel()

		createTestStream(t, client, ctx, "queue-a", "jobset-1", 100)
		createTestStream(t, client, ctx, "queue-a", "jobset-2", 200)
		createTestStream(t, client, ctx, "queue-a", "jobset-3", 300)
		createTestStream(t, client, ctx, "queue-b", "jobset-1", 50)
		createTestStream(t, client, ctx, "queue-b", "jobset-2", 75)
		createTestStream(t, client, ctx, "queue-b", "jobset-3", 125)
		createTestStream(t, client, ctx, "queue-c", "jobset-1", 10)
		createTestStream(t, client, ctx, "queue-c", "jobset-2", 20)
		createTestStream(t, client, ctx, "queue-c", "jobset-3", 30)
		createTestStream(t, client, ctx, "queue-c", "jobset-4", 40)

		config := Config{
			CollectionInterval: 5 * time.Minute,
			TopN:               5,
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}
		scanner := NewScanner(client, config)
		collector := NewCollector(scanner, config, leader.NewStandaloneLeaderController())

		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 10)

		queueCounts := make(map[string]int)
		queueEventTotals := make(map[string]int64)
		for _, s := range streams {
			queueCounts[s.Queue]++
			queueEventTotals[s.Queue] += s.Length
		}

		assert.Equal(t, 3, queueCounts["queue-a"])
		assert.Equal(t, 3, queueCounts["queue-b"])
		assert.Equal(t, 4, queueCounts["queue-c"])
		assert.Equal(t, int64(600), queueEventTotals["queue-a"])
		assert.Equal(t, int64(250), queueEventTotals["queue-b"])
		assert.Equal(t, int64(100), queueEventTotals["queue-c"])

		sort.Slice(streams, func(i, j int) bool {
			return streams[i].Length > streams[j].Length
		})
		topNByLength := streams[:5]
		assert.Equal(t, int64(300), topNByLength[0].Length)
		assert.Equal(t, int64(200), topNByLength[1].Length)
		assert.Equal(t, int64(125), topNByLength[2].Length)

		sort.Slice(streams, func(i, j int) bool {
			return streams[i].AgeSeconds > streams[j].AgeSeconds
		})
		for _, s := range streams {
			assert.Greater(t, s.AgeSeconds, 0.0)
		}

		descCh := make(chan *prometheus.Desc, 100)
		go func() {
			collector.Describe(descCh)
			close(descCh)
		}()

		descCount := 0
		for range descCh {
			descCount++
		}
		assert.Equal(t, 13, descCount)
	})
}

func TestIntegration_EmptyRedis(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		config := Config{
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
			TopN:               100,
		}
		scanner := NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 0)
	})
}

func TestIntegration_KeyExpiryMidScan(t *testing.T) {
	withRedisClient(t, func(client redis.UniversalClient) {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
		defer cancel()

		createTestStream(t, client, ctx, "queue-persist", "jobset-1", 10)
		createTestStream(t, client, ctx, "queue-persist", "jobset-2", 20)

		expiringKey := createTestStream(t, client, ctx, "queue-expiring", "jobset-ttl", 5)
		err := client.Expire(ctx, expiringKey, 1*time.Second).Err()
		require.NoError(t, err)

		createTestStream(t, client, ctx, "queue-persist", "jobset-3", 30)
		time.Sleep(2 * time.Second)

		exists, err := client.Exists(ctx, expiringKey).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists)

		config := Config{
			ScanBatchSize:      1000,
			PipelineBatchSize:  500,
			InterBatchDelay:    0,
			MemoryUsageSamples: 5,
		}
		scanner := NewScanner(client, config)
		streams, err := scanner.ScanAll(ctx)
		require.NoError(t, err)
		require.Len(t, streams, 3)

		streamKeys := make(map[string]bool)
		for _, s := range streams {
			streamKeys[s.Key] = true
		}
		assert.True(t, streamKeys["Events:queue-persist:jobset-1"])
		assert.True(t, streamKeys["Events:queue-persist:jobset-2"])
		assert.True(t, streamKeys["Events:queue-persist:jobset-3"])
		assert.False(t, streamKeys[expiringKey])
	})
}
