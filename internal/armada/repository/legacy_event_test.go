package repository

import (
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common/compress"

	"github.com/gogo/protobuf/proto"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/pkg/api"
)

func TestCheckStreamExists(t *testing.T) {
	withLegacyRedisEventRepository(func(r *LegacyRedisEventRepository) {
		exists, err := r.CheckStreamExists("test", "jobset")
		assert.NoError(t, err)
		assert.False(t, exists)

		event := createEvent("test", "jobset", time.Now())
		err = r.ReportEvents([]*api.EventMessage{event})
		assert.NoError(t, err)

		exists, err = r.CheckStreamExists("test", "jobset")
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestReadEvents(t *testing.T) {
	withLegacyRedisEventRepository(func(r *LegacyRedisEventRepository) {
		created := time.Now().UTC()
		event := createEvent("test", "jobset", created)
		err := r.ReportEvents([]*api.EventMessage{event})
		assert.NoError(t, err)

		events, err := r.ReadEvents("test", "jobset", "", 500, 1*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(events))
		assert.Equal(t, createEvent("test", "jobset", created), events[0].Message)
	})
}

func TestFailedEventCompressed(t *testing.T) {
	created := time.Now().UTC()
	failedEvent := &api.EventMessage{
		Events: &api.EventMessage_Failed{
			Failed: &api.JobFailedEvent{
				JobId:    "jobId",
				JobSetId: "test-compressed2",
				Queue:    "test",
				Created:  created,
			},
		},
	}

	withLegacyRedisEventRepository(func(r *LegacyRedisEventRepository) {
		err := r.ReportEvents([]*api.EventMessage{failedEvent})
		assert.NoError(t, err)
		// This is a bit annoying- ReportEvents nulls out jobset and queue.
		// put them back here
		failedEvent.GetFailed().Queue = "test"
		failedEvent.GetFailed().JobSetId = "test-compressed2"
		events, err := r.ReadEvents("test", "test-compressed2", "", 500, 1*time.Second)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(events))
		assert.Equal(t, failedEvent, events[0].Message)

		// bonus test: check that the data is actually compressed in redis
		cmd, err := r.db.XRead(&redis.XReadArgs{
			Streams: []string{getJobSetEventsKey("test", "test-compressed2"), "0"},
			Count:   500,
			Block:   1 * time.Second,
		}).Result()
		assert.NoError(t, err)

		data := cmd[0].Messages[0].Values[dataKey]
		msg := &api.EventMessage{}
		bytes := []byte(data.(string))
		err = proto.Unmarshal(bytes, msg)
		assert.NoError(t, err)

		// check that event contains compressed data
		msg.GetFailedCompressed().GetEvent()
		decompressor, err := compress.NewZlibDecompressor()
		assert.NoError(t, err)
		_, err = decompressor.Decompress(msg.GetFailedCompressed().Event)
		assert.NoError(t, err)
	})
}

func createEvent(queue string, jobSetId string, created time.Time) *api.EventMessage {
	return &api.EventMessage{
		Events: &api.EventMessage_Running{
			Running: &api.JobRunningEvent{
				JobId:    "jobId",
				JobSetId: jobSetId,
				Queue:    queue,
				Created:  created,
			},
		},
	}
}

func withLegacyRedisEventRepository(action func(r *LegacyRedisEventRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewLegacyRedisEventRepository(client, configuration.EventRetentionPolicy{ExpiryEnabled: true, RetentionDuration: time.Hour})
	action(repo)
}
