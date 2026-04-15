package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
)

// TestScanAll_SingleKey tests scanning with a single stream key using real Redis.
func TestScanAll_SingleKey(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		key := seedRedisStream(t, client, ctx, "myqueue", "myjobset", 100)

		config := configuration.RedisMemoryMetricsConfig{
			ScanBatchSize:     10,
			PipelineBatchSize: 5,
			InterBatchDelay:   0,
		}

		scanner := NewScanner(client, config)
		results, err := scanner.ScanAll(ctx)

		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		assert.Equal(t, key, result.Key)
		assert.Equal(t, "myqueue", result.Queue)
		assert.Equal(t, "myjobset", result.JobSetId)
		assert.Equal(t, int64(100), result.Length)
		assert.Greater(t, result.MemoryBytes, int64(0))
		assert.NotEmpty(t, result.FirstEntryID)
		assert.NotEmpty(t, result.LastEntryID)
		assert.GreaterOrEqual(t, result.AgeSeconds, float64(0))
	})
}

// TestScanAll_MultipleBatches tests scanning with multiple pipeline batches.
func TestScanAll_MultipleBatches(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	withRedisClient(ctx, func(client redis.UniversalClient) {
		// Create 15 streams with varying entry counts to force multiple pipeline batches
		// With PipelineBatchSize: 5, we'll have 3 batches (15 streams / 5 per batch)
		for i := 0; i < 15; i++ {
			queue := fmt.Sprintf("queue%d", i)
			jobSetId := fmt.Sprintf("jobset%d", i)
			entryCount := 10 * (i + 1) // 10, 20, 30, ..., 150
			seedRedisStream(t, client, ctx, queue, jobSetId, entryCount)
		}

		config := configuration.RedisMemoryMetricsConfig{
			ScanBatchSize:     20,
			PipelineBatchSize: 5,
			InterBatchDelay:   0,
		}

		scanner := NewScanner(client, config)
		results, err := scanner.ScanAll(ctx)

		require.NoError(t, err)
		require.Len(t, results, 15)

		// Verify each stream has correct length and memory bytes
		for i := 0; i < 15; i++ {
			expectedKey := fmt.Sprintf("Events:queue%d:jobset%d", i, i)
			expectedLength := int64(10 * (i + 1))

			// Find the result for this stream
			var found *StreamInfo
			for j := range results {
				if results[j].Key == expectedKey {
					found = &results[j]
					break
				}
			}

			require.NotNil(t, found, "stream %s not found in results", expectedKey)
			assert.Equal(t, expectedKey, found.Key)
			assert.Equal(t, expectedLength, found.Length)
			assert.Greater(t, found.MemoryBytes, int64(0))
		}
	})
}

// TestScanAll_ContextCancelled tests that scanning respects context cancellation.
func TestScanAll_ContextCancelled(t *testing.T) {
	seedCtx, seedCancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer seedCancel()
	withRedisClient(seedCtx, func(client redis.UniversalClient) {
		seedRedisStream(t, client, seedCtx, "myqueue", "myjobset", 100)

		config := configuration.RedisMemoryMetricsConfig{
			ScanBatchSize:     10,
			PipelineBatchSize: 5,
			InterBatchDelay:   100 * time.Millisecond,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		scanner := NewScanner(client, config)
		_, err := scanner.ScanAll(ctx)

		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

func withRedisClient(ctx *armadacontext.Context, action func(client redis.UniversalClient)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 11})
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
