package repository

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestStoreAndGetPulsarSchedulerJobDetails(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	withRepository(ctx, func(r *RedisJobRepository) {
		details := &schedulerobjects.PulsarSchedulerJobDetails{
			JobId:  util.NewULID(),
			Queue:  "testQueue",
			JobSet: "testJobset",
		}
		err := r.StorePulsarSchedulerJobDetails(ctx, []*schedulerobjects.PulsarSchedulerJobDetails{details})
		require.NoError(t, err)

		retrievedDetails, err := r.GetPulsarSchedulerJobDetails(ctx, details.JobId)
		require.NoError(t, err)
		assert.Equal(t, details, retrievedDetails)

		nonExistantDetails, err := r.GetPulsarSchedulerJobDetails(ctx, "not a valid details key")
		require.NoError(t, err)
		assert.Nil(t, nonExistantDetails)
	})
}

func withRepository(ctx *armadacontext.Context, action func(r *RedisJobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB(ctx)
	defer client.Close()
	client.FlushDB(ctx)
	repo := NewRedisJobRepository(client)
	action(repo)
}
