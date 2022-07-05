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

		event := createEvent("test", "jobset")
		err = r.ReportEvents([]*api.EventMessage{event})
		assert.NoError(t, err)

		exists, err = r.CheckStreamExists("test", "jobset")
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func createEvent(queue string, jobSetId string) *api.EventMessage {
	return &api.EventMessage{
		Events: &api.EventMessage_Running{
			Running: &api.JobRunningEvent{
				JobId:    "jobId",
				JobSetId: jobSetId,
				Queue:    queue,
				Created:  time.Now(),
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
