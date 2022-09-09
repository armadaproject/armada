package store

import (
	"github.com/G-Research/armada/internal/eventingester/configuration"
	"github.com/G-Research/armada/internal/eventingester/model"

	"github.com/go-redis/redis"
)

const (
	eventStreamPrefix = "Events:"
	dataKey           = "message"
)

type EventStore interface {
	ReportEvents(update []*model.Event) error
}

type RedisEventStore struct {
	db             redis.UniversalClient
	eventRetention configuration.EventRetentionPolicy
}

func NewRedisEventStore(db redis.UniversalClient, eventRetention configuration.EventRetentionPolicy) *RedisEventStore {
	return &RedisEventStore{db: db, eventRetention: eventRetention}
}

func (repo *RedisEventStore) ReportEvents(update []*model.Event) error {
	if len(update) == 0 {
		return nil
	}

	type eventData struct {
		key  string
		data []byte
	}
	var data []eventData
	uniqueJobSets := make(map[string]bool)

	for _, e := range update {
		key := getJobSetEventsKey(e.Queue, e.Jobset)
		data = append(data, eventData{key: key, data: e.Event})
		uniqueJobSets[key] = true
	}

	pipe := repo.db.Pipeline()
	for _, e := range data {
		pipe.XAdd(&redis.XAddArgs{
			Stream: e.key,
			Values: map[string]interface{}{
				dataKey: e.data,
			},
		})
	}

	if repo.eventRetention.ExpiryEnabled {
		for key := range uniqueJobSets {
			pipe.Expire(key, repo.eventRetention.RetentionDuration)
		}
	}

	_, e := pipe.Exec()
	return e
}

func getJobSetEventsKey(queue, jobSetId string) string {
	return eventStreamPrefix + queue + ":" + jobSetId
}
