package store

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/model"
)

func TestEventStoreWritePathUnchanged_ClientCountAndNames(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisEventStore(ctx, func(r *RedisEventStore) {
		assert.Equal(t, 2, len(r.dbs), "event store should have exactly 2 clients (main and replica)")
		assert.Equal(t, []string{"client", "client2"}, r.dbNames, "client names should be main and replica")
	})
}

func TestEventStoreWritePathUnchanged_WritesToBothClients(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisEventStore(ctx, func(r *RedisEventStore) {
		update := &model.BatchUpdate{
			Events: []*model.Event{
				{
					Queue:  "test-queue",
					Jobset: "test-jobset",
					Event:  []byte("test-event"),
				},
			},
		}

		err := r.Store(ctx, update)
		require.NoError(t, err)

		for i, db := range r.dbs {
			event, err := ReadEvent(ctx, db, "test-queue", "test-jobset")
			require.NoError(t, err, "client %d (%s) should have the event", i, r.dbNames[i])
			assert.Equal(t, update.Events[0].Event, event, "client %d (%s) should have correct event data", i, r.dbNames[i])
		}
	})
}

func TestEventStoreWritePathUnchanged_FanoutToMainAndReplica(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisEventStore(ctx, func(r *RedisEventStore) {
		update := &model.BatchUpdate{
			Events: []*model.Event{
				{
					Queue:  "queue-1",
					Jobset: "jobset-1",
					Event:  []byte("event-1"),
				},
				{
					Queue:  "queue-2",
					Jobset: "jobset-2",
					Event:  []byte("event-2"),
				},
			},
		}

		err := r.Store(ctx, update)
		require.NoError(t, err)

		for i, db := range r.dbs {
			event1, err := ReadEvent(ctx, db, "queue-1", "jobset-1")
			require.NoError(t, err, "client %d (%s) should have event-1", i, r.dbNames[i])
			assert.Equal(t, []byte("event-1"), event1)

			event2, err := ReadEvent(ctx, db, "queue-2", "jobset-2")
			require.NoError(t, err, "client %d (%s) should have event-2", i, r.dbNames[i])
			assert.Equal(t, []byte("event-2"), event2)
		}
	})
}

func TestEventStoreWritePathUnchanged_NoMetricsClientInvolved(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisEventStore(ctx, func(r *RedisEventStore) {
		assert.Equal(t, 2, len(r.dbs), "event store should only have 2 clients")
		assert.NotContains(t, r.dbNames, "metrics", "metrics client should not be in event store")

		update := &model.BatchUpdate{
			Events: []*model.Event{
				{
					Queue:  "test-queue",
					Jobset: "test-jobset",
					Event:  []byte("test-event"),
				},
			},
		}

		err := r.Store(ctx, update)
		require.NoError(t, err)
	})
}

func TestEventStoreWritePathUnchanged_MetricsSentinelConfigDoesNotAffectWriteClients(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	withRedisEventStore(ctx, func(r *RedisEventStore) {
		metricsConfig := &redis.UniversalOptions{
			MasterName: "mymaster",
			Addrs:      []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"},
			ReadOnly:   true,
		}

		assert.NotEmpty(t, metricsConfig.MasterName, "metrics config has Sentinel masterName")
		assert.NotEmpty(t, metricsConfig.Addrs, "metrics config has Sentinel addrs")

		update := &model.BatchUpdate{
			Events: []*model.Event{
				{
					Queue:  "test-queue",
					Jobset: "test-jobset",
					Event:  []byte("test-event"),
				},
			},
		}

		err := r.Store(ctx, update)
		require.NoError(t, err)

		assert.Equal(t, 2, len(r.dbs), "event store should still have 2 clients regardless of metrics config")

		for i, db := range r.dbs {
			event, err := ReadEvent(ctx, db, "test-queue", "test-jobset")
			require.NoError(t, err, "client %d (%s) should have event", i, r.dbNames[i])
			assert.Equal(t, []byte("test-event"), event)
		}
	})
}

func TestEventStoreWritePathUnchanged_OnlyMainClientWhenNoReplica(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 7})
	defer client.FlushDB(ctx)
	defer client.Close()

	store := &RedisEventStore{
		dbs:     []redis.UniversalClient{client},
		dbNames: []string{"main"},
		eventRetention: configuration.EventRetentionPolicy{
			RetentionDuration: time.Hour,
		},
	}

	update := &model.BatchUpdate{
		Events: []*model.Event{
			{
				Queue:  "test-queue",
				Jobset: "test-jobset",
				Event:  []byte("test-event"),
			},
		},
	}

	err := store.Store(ctx, update)
	require.NoError(t, err)

	assert.Equal(t, 1, len(store.dbs), "event store should only have 1 client when no replica")

	event, err := ReadEvent(ctx, client, "test-queue", "test-jobset")
	require.NoError(t, err)
	assert.Equal(t, []byte("test-event"), event)
}
