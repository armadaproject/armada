package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/constants"
)

func withRedisClient(ctx *armadacontext.Context, action func(client redis.UniversalClient)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
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
