package redismetrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SimpleMockPipeliner provides a lightweight mock for Pipeline
type SimpleMockPipeliner struct {
	xInfoData  map[int]*redis.XInfoStream
	memData    map[int]int64
	vanished   map[int]bool
	cmdIdx     int
	cmdKeys    []string // Track which key was used for each command
	keyResults map[string]*redis.XInfoStream
	memResults map[string]int64
	// Embed a nil field to satisfy redis.Pipeliner interface methods we don't override
	redis.Pipeliner
}

func (m *SimpleMockPipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	var cmders []redis.Cmder
	for i := 0; i < m.cmdIdx; i++ {
		if i%2 == 0 {
			// This is an XInfoStream command
			infoCmd := &redis.XInfoStreamCmd{}
			if i/2 < len(m.cmdKeys) {
				key := m.cmdKeys[i/2]
				if m.vanished[i] {
					infoCmd.SetErr(redis.Nil)
				} else if info, ok := m.keyResults[key]; ok {
					infoCmd.SetVal(info)
				} else {
					infoCmd.SetErr(redis.Nil)
				}
			} else {
				infoCmd.SetErr(redis.Nil)
			}
			cmders = append(cmders, infoCmd)
		} else {
			// This is a MemoryUsage command
			memCmd := &redis.IntCmd{}
			memIdx := (i - 1) / 2
			if memIdx < len(m.cmdKeys) {
				key := m.cmdKeys[memIdx]
				if m.vanished[i] {
					memCmd.SetErr(redis.Nil)
				} else if mem, ok := m.memResults[key]; ok {
					memCmd.SetVal(mem)
				} else {
					memCmd.SetErr(redis.Nil)
				}
			} else {
				memCmd.SetErr(redis.Nil)
			}
			cmders = append(cmders, memCmd)
		}
	}
	return cmders, nil
}

func (m *SimpleMockPipeliner) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	m.cmdIdx++
	// Track which key is being queried
	if len(m.cmdKeys) <= m.cmdIdx/2 {
		m.cmdKeys = append(m.cmdKeys, key)
	}
	return &redis.XInfoStreamCmd{}
}

func (m *SimpleMockPipeliner) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	m.cmdIdx++
	return &redis.IntCmd{}
}

func (m *SimpleMockPipeliner) Discard() {}

// SimpleMockRedisClient is a minimal mock of RedisClient
type SimpleMockRedisClient struct {
	scanResults map[uint64]struct {
		keys       []string
		nextCursor uint64
	}
	xInfoResults  map[string]*redis.XInfoStream
	memoryResults map[string]int64
	pipeliner     *SimpleMockPipeliner
}

func NewSimpleMockRedisClient() *SimpleMockRedisClient {
	return &SimpleMockRedisClient{
		scanResults: make(map[uint64]struct {
			keys       []string
			nextCursor uint64
		}),
		xInfoResults:  make(map[string]*redis.XInfoStream),
		memoryResults: make(map[string]int64),
		pipeliner: &SimpleMockPipeliner{
			xInfoData:  make(map[int]*redis.XInfoStream),
			memData:    make(map[int]int64),
			vanished:   make(map[int]bool),
			keyResults: make(map[string]*redis.XInfoStream),
			memResults: make(map[string]int64),
		},
	}
}

func (m *SimpleMockRedisClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	cmd := &redis.ScanCmd{}
	result, ok := m.scanResults[cursor]
	if !ok {
		result = struct {
			keys       []string
			nextCursor uint64
		}{keys: []string{}, nextCursor: 0}
	}
	cmd.SetVal(result.keys, result.nextCursor)
	return cmd
}

func (m *SimpleMockRedisClient) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	cmd := &redis.XInfoStreamCmd{}
	if info, ok := m.xInfoResults[key]; ok {
		cmd.SetVal(info)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (m *SimpleMockRedisClient) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	if memBytes, ok := m.memoryResults[key]; ok {
		cmd.SetVal(memBytes)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (m *SimpleMockRedisClient) Pipeline() redis.Pipeliner {
	newPipe := &SimpleMockPipeliner{
		xInfoData:  m.pipeliner.xInfoData,
		memData:    m.pipeliner.memData,
		vanished:   m.pipeliner.vanished,
		cmdIdx:     0,
		cmdKeys:    []string{},
		keyResults: m.xInfoResults,
		memResults: m.memoryResults,
	}
	return newPipe
}

// TestScanAll_EmptyRedis tests scanning when Redis is empty.
func TestScanAll_EmptyRedis(t *testing.T) {
	client := NewSimpleMockRedisClient()
	client.scanResults[0] = struct {
		keys       []string
		nextCursor uint64
	}{keys: []string{}, nextCursor: 0}

	config := Config{
		ScanBatchSize:     10,
		PipelineBatchSize: 5,
		InterBatchDelay:   0,
	}

	scanner := NewScanner(client, config)
	results, err := scanner.ScanAll(context.Background())

	require.NoError(t, err)
	assert.Empty(t, results)
}

// TestScanAll_SingleKey tests scanning with a single stream key.
func TestScanAll_SingleKey(t *testing.T) {
	client := NewSimpleMockRedisClient()

	key := "Events:myqueue:myjobset"
	now := time.Now()
	msStr := fmt.Sprintf("%d", now.UnixMilli())

	client.scanResults[0] = struct {
		keys       []string
		nextCursor uint64
	}{keys: []string{key}, nextCursor: 0}

	client.xInfoResults[key] = &redis.XInfoStream{
		Length: 100,
		FirstEntry: redis.XMessage{
			ID: msStr + "-0",
		},
		LastEntry: redis.XMessage{
			ID: msStr + "-99",
		},
	}

	client.memoryResults[key] = 1024

	client.pipeliner.xInfoData[0] = client.xInfoResults[key]
	client.pipeliner.memData[1] = 1024

	config := Config{
		ScanBatchSize:     10,
		PipelineBatchSize: 5,
		InterBatchDelay:   0,
	}

	scanner := NewScanner(client, config)
	results, err := scanner.ScanAll(context.Background())

	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, key, results[0].Key)
	assert.Equal(t, "myqueue", results[0].Queue)
	assert.Equal(t, "myjobset", results[0].JobSetId)
	assert.Equal(t, int64(100), results[0].Length)
	assert.Equal(t, int64(1024), results[0].MemoryBytes)
	assert.Equal(t, msStr+"-0", results[0].FirstEntryID)
	assert.Equal(t, msStr+"-99", results[0].LastEntryID)
	assert.True(t, results[0].AgeSeconds >= 0)
}

// TestScanAll_KeyVanishesMidScan tests that vanished keys are skipped.
func TestScanAll_KeyVanishesMidScan(t *testing.T) {
	client := NewSimpleMockRedisClient()

	key1 := "Events:queue1:jobset1"
	key2 := "Events:queue2:jobset2"
	now := time.Now()
	msStr := fmt.Sprintf("%d", now.UnixMilli())

	client.scanResults[0] = struct {
		keys       []string
		nextCursor uint64
	}{keys: []string{key1, key2}, nextCursor: 0}

	client.xInfoResults[key1] = &redis.XInfoStream{
		Length: 50,
		FirstEntry: redis.XMessage{
			ID: msStr + "-0",
		},
		LastEntry: redis.XMessage{
			ID: msStr + "-49",
		},
	}

	client.memoryResults[key1] = 512

	client.pipeliner.xInfoData[0] = client.xInfoResults[key1]
	client.pipeliner.memData[1] = 512
	client.pipeliner.vanished[2] = true

	config := Config{
		ScanBatchSize:     10,
		PipelineBatchSize: 5,
		InterBatchDelay:   0,
	}

	scanner := NewScanner(client, config)
	results, err := scanner.ScanAll(context.Background())

	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, key1, results[0].Key)
}

// TestScanAll_MultipleBatches tests scanning with multiple pipeline batches.
func TestScanAll_MultipleBatches(t *testing.T) {
	client := NewSimpleMockRedisClient()

	var keys []string
	now := time.Now()
	msStr := fmt.Sprintf("%d", now.UnixMilli())

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("Events:queue%d:jobset%d", i, i)
		keys = append(keys, key)

		client.xInfoResults[key] = &redis.XInfoStream{
			Length: int64(10 * (i + 1)),
			FirstEntry: redis.XMessage{
				ID: msStr + "-0",
			},
			LastEntry: redis.XMessage{
				ID: fmt.Sprintf("%s-%d", msStr, 10*(i+1)-1),
			},
		}

		client.memoryResults[key] = int64(1024 * (i + 1))
	}

	client.scanResults[0] = struct {
		keys       []string
		nextCursor uint64
	}{keys: keys, nextCursor: 0}

	config := Config{
		ScanBatchSize:     20,
		PipelineBatchSize: 5,
		InterBatchDelay:   0,
	}

	scanner := NewScanner(client, config)
	results, err := scanner.ScanAll(context.Background())

	require.NoError(t, err)
	require.Len(t, results, 15)

	for i := 0; i < 15; i++ {
		assert.Equal(t, keys[i], results[i].Key)
		assert.Equal(t, int64(10*(i+1)), results[i].Length)
		assert.Equal(t, int64(1024*(i+1)), results[i].MemoryBytes)
	}
}

// TestScanAll_ContextCancelled tests that scanning respects context cancellation.
func TestScanAll_ContextCancelled(t *testing.T) {
	client := NewSimpleMockRedisClient()

	key := "Events:myqueue:myjobset"
	client.scanResults[0] = struct {
		keys       []string
		nextCursor uint64
	}{keys: []string{key}, nextCursor: 0}

	config := Config{
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
}
