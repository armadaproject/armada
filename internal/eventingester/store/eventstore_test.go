package store

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/model"
)

func TestReportEvents(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()
	withRedisEventStore(ctx, func(r *RedisEventStore) {
		update := &model.BatchUpdate{
			Events: []*model.Event{
				{
					Queue:  "testQueue",
					Jobset: "testJobset",
					Event:  []byte{1},
				},
				{
					Queue:  "testQueue",
					Jobset: "testJobset2",
					Event:  []byte{2},
				},
			},
		}

		err := r.Store(armadacontext.Background(), update)
		assert.NoError(t, err)

		read1, err := ReadEvent(ctx, r.db, "testQueue", "testJobset")
		assert.NoError(t, err)
		assert.Equal(t, update.Events[0].Event, read1)

		read2, err := ReadEvent(ctx, r.db, "testQueue", "testJobset2")
		assert.NoError(t, err)
		assert.Equal(t, update.Events[1].Event, read2)
	})
}

func withRedisEventStore(ctx *armadacontext.Context, action func(es *RedisEventStore)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB(ctx)
	defer client.Close()

	client.FlushDB(ctx)
	repo := &RedisEventStore{
		db: client,
		eventRetention: configuration.EventRetentionPolicy{
			RetentionDuration: time.Hour,
		},
	}
	action(repo)
}

func ReadEvent(ctx *armadacontext.Context, r redis.UniversalClient, queue string, jobset string) ([]byte, error) {
	cmd, err := r.XRead(ctx, &redis.XReadArgs{
		Streams: []string{getJobSetEventsKey(queue, jobset), "0"},
		Count:   500,
		Block:   1 * time.Second,
	}).Result()
	if err != nil {
		return nil, err
	}
	return []byte(cmd[0].Messages[0].Values[dataKey].(string)), nil
}
