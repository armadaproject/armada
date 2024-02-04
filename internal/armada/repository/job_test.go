package repository

import (
	"testing"

	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAndGetPulsarSchedulerJobDetails(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		details := &schedulerobjects.PulsarSchedulerJobDetails{
			JobId:  util.NewULID(),
			Queue:  "testQueue",
			JobSet: "testJobset",
		}
		err := r.StorePulsarSchedulerJobDetails([]*schedulerobjects.PulsarSchedulerJobDetails{details})
		require.NoError(t, err)

		retrievedDetails, err := r.GetPulsarSchedulerJobDetails(details.JobId)
		require.NoError(t, err)
		assert.Equal(t, details, retrievedDetails)

		nonExistantDetails, err := r.GetPulsarSchedulerJobDetails("not a valid details key")
		require.NoError(t, err)
		assert.Nil(t, nonExistantDetails)
	})
}

func withRepository(action func(r *RedisJobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()
	client.FlushDB()
	repo := NewRedisJobRepository(client)
	action(repo)
}
