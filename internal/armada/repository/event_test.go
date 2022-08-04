package repository

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/pkg/api"
)

func TestCheckStreamExists(t *testing.T) {
	withRedisEventRepository(func(r *RedisEventRepository) {
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

	withRedisEventRepository(func(r *RedisEventRepository) {
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

func withRedisEventRepository(action func(r *RedisEventRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewRedisEventRepository(client, configuration.EventRetentionPolicy{ExpiryEnabled: true, RetentionDuration: time.Hour})
	action(repo)
}
