package redismetrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config contains configuration for Redis memory metrics collection.
type Config struct {
	Enabled            bool
	CollectionInterval time.Duration
	TopN               int
	ScanBatchSize      int64
	PipelineBatchSize  int
	InterBatchDelay    time.Duration
	MemoryUsageSamples int
}

// StreamInfo contains information about a single Redis stream.
type StreamInfo struct {
	Key          string
	Queue        string
	JobSetId     string
	Length       int64
	MemoryBytes  int64
	FirstEntryID string
	LastEntryID  string
	AgeSeconds   float64
}

// RedisClient interface wraps Redis operations needed for memory metrics collection.
type RedisClient interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd
	MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd
	Pipeline() redis.Pipeliner
}

// Pipeliner interface for pipelining commands
type Pipeliner interface {
	XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd
	MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd
	Exec(ctx context.Context) ([]redis.Cmder, error)
	Discard()
}

// parseStreamKey parses a Redis stream key in the format "Events:{queue}:{jobSetId}"
// and returns the queue and jobSetId components.
// The queue name may contain ':' characters, so we only split on the first ':' after the prefix.
func parseStreamKey(key string) (queue, jobSetId string, err error) {
	const prefix = "Events:"

	// Check if the key has the correct prefix
	if !strings.HasPrefix(key, prefix) {
		return "", "", fmt.Errorf("stream key does not have %q prefix: %s", prefix, key)
	}

	// Strip the prefix
	rest := strings.TrimPrefix(key, prefix)

	// Split on the first ':' to separate queue from jobSetId
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("stream key format invalid: expected {queue}:{jobSetId}, got %s", rest)
	}

	return parts[0], parts[1], nil
}
