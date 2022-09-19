package store

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/eventingester/configuration"
	"github.com/G-Research/armada/internal/eventingester/model"
)

func TestReportEvents(t *testing.T) {
	withRedisEventStore(func(r *RedisEventStore) {
		event1 := &model.Event{
			Queue:  "testQueue",
			Jobset: "testJobset",
			Event:  []byte{1},
		}
		event2 := &model.Event{
			Queue:  "testQueue",
			Jobset: "testJobset2",
			Event:  []byte{2},
		}

		err := r.ReportEvents([]*model.Event{event1, event2})
		assert.NoError(t, err)

		read1, err := ReadEvent(r.db, "testQueue", "testJobset")
		assert.NoError(t, err)
		assert.Equal(t, event1.Event, read1)

		read2, err := ReadEvent(r.db, "testQueue", "testJobset2")
		assert.NoError(t, err)
		assert.Equal(t, event2.Event, read2)
	})
}

func withRedisEventStore(action func(es *RedisEventStore)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewRedisEventStore(client, configuration.EventRetentionPolicy{ExpiryEnabled: true, RetentionDuration: time.Hour})
	action(repo)
}

func ReadEvent(r redis.UniversalClient, queue string, jobset string) ([]byte, error) {
	cmd, err := r.XRead(&redis.XReadArgs{
		Streams: []string{getJobSetEventsKey(queue, jobset), "0"},
		Count:   500,
		Block:   1 * time.Second,
	}).Result()
	if err != nil {
		return nil, err
	}
	return []byte(cmd[0].Messages[0].Values[dataKey].(string)), nil
}
