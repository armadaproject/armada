package redismetrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func withRedisClient(t *testing.T, action func(client redis.UniversalClient)) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 60*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.Close()

	err := client.FlushDB(ctx).Err()
	require.NoError(t, err)

	action(client)

	err = client.FlushDB(ctx).Err()
	require.NoError(t, err)
}

func seedRedisStream(t *testing.T, client redis.UniversalClient, ctx context.Context, queue, jobSetId string, entryCount int) string {
	streamKey := fmt.Sprintf("Events:%s:%s", queue, jobSetId)

	for i := 0; i < entryCount; i++ {
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{"index": i},
		}).Result()
		require.NoError(t, err)
	}
	return streamKey
}
