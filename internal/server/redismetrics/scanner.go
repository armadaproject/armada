package redismetrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Scanner scans Redis streams and collects memory usage metrics.
type Scanner struct {
	client RedisClient
	config Config
}

// NewScanner creates a new Scanner instance.
func NewScanner(client RedisClient, config Config) *Scanner {
	return &Scanner{
		client: client,
		config: config,
	}
}

// ScanAll scans all Redis streams and returns information about them.
func (s *Scanner) ScanAll(ctx context.Context) ([]StreamInfo, error) {
	var results []StreamInfo
	cursor := uint64(0)

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Scan for keys matching the Events: pattern
		cmd := s.client.Scan(ctx, cursor, "Events:*", s.config.ScanBatchSize)
		keys, newCursor, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		// Process keys in batches with pipeline
		if len(keys) > 0 {
			batchResults, err := s.processBatch(ctx, keys)
			if err != nil {
				return nil, err
			}
			results = append(results, batchResults...)
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}

		// Sleep between scans if there are more batches
		if s.config.InterBatchDelay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(s.config.InterBatchDelay):
			}
		}
	}

	return results, nil
}

// processBatch processes a batch of keys using pipelined commands.
func (s *Scanner) processBatch(ctx context.Context, keys []string) ([]StreamInfo, error) {
	var results []StreamInfo

	// Process in pipeline batches
	for i := 0; i < len(keys); i += s.config.PipelineBatchSize {
		end := i + s.config.PipelineBatchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		batchResults, err := s.executePipelineBatch(ctx, batch)
		if err != nil {
			return nil, err
		}
		results = append(results, batchResults...)

		// Sleep between batches if configured
		if s.config.InterBatchDelay > 0 && end < len(keys) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(s.config.InterBatchDelay):
			}
		}
	}

	return results, nil
}

// executePipelineBatch executes a single pipeline batch of commands.
func (s *Scanner) executePipelineBatch(ctx context.Context, keys []string) ([]StreamInfo, error) {
	pipe := s.client.Pipeline()

	// Add commands to pipeline
	for _, key := range keys {
		pipe.XInfoStream(ctx, key)
		pipe.MemoryUsage(ctx, key, s.config.MemoryUsageSamples)
	}

	// Execute pipeline
	cmders, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("pipeline execution error: %w", err)
	}

	// Process results
	var results []StreamInfo
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		infoIdx := i * 2
		memIdx := i*2 + 1

		// Get XINFO result
		infoCmd, ok := cmders[infoIdx].(*redis.XInfoStreamCmd)
		if !ok {
			continue
		}

		info, err := infoCmd.Result()
		if err != nil {
			// Skip keys that vanished or have other issues
			if err == redis.Nil || strings.Contains(err.Error(), "NOSTREAM") {
				continue
			}
			return nil, fmt.Errorf("xinfo error for key %s: %w", key, err)
		}

		// Get MemoryUsage result
		memCmd, ok := cmders[memIdx].(*redis.IntCmd)
		if !ok {
			continue
		}

		memBytes, err := memCmd.Result()
		if err != nil {
			// Skip keys that vanished
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("memory usage error for key %s: %w", key, err)
		}

		// Parse the stream key to get queue and jobSetId
		queue, jobSetId, err := parseStreamKey(key)
		if err != nil {
			// Skip keys that don't match the expected format
			continue
		}

		// Compute age from FirstEntryID
		ageSeconds := computeAge(info.FirstEntry)

		// Create StreamInfo
		firstEntryID := info.FirstEntry.ID
		lastEntryID := info.LastEntry.ID

		streamInfo := StreamInfo{
			Key:          key,
			Queue:        queue,
			JobSetId:     jobSetId,
			Length:       info.Length,
			MemoryBytes:  memBytes,
			FirstEntryID: firstEntryID,
			LastEntryID:  lastEntryID,
			AgeSeconds:   ageSeconds,
		}

		results = append(results, streamInfo)
	}

	return results, nil
}

// computeAge computes the age of a stream from its first entry ID.
// The ID format is "{milliseconds}-{sequenceNumber}".
func computeAge(entry redis.XMessage) float64 {
	id := entry.ID
	if id == "" {
		return 0
	}

	parts := strings.Split(id, "-")
	if len(parts) < 1 {
		return 0
	}

	msStr := parts[0]
	ms, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil {
		return 0
	}

	now := time.Now()
	entryTime := time.UnixMilli(ms)
	age := now.Sub(entryTime).Seconds()

	if age < 0 {
		return 0
	}

	return age
}
