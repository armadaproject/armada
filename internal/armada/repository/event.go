package repository

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/pkg/api"
)

const eventStreamPrefix = "Events:"
const dataKey = "message"

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}

type EventRepository interface {
	ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
	GetLastMessageId(queue, jobSetId string) (string, error)
}

type RedisEventRepository struct {
	db             redis.UniversalClient
	eventRetention configuration.EventRetentionPolicy
}

func NewRedisEventRepository(db redis.UniversalClient, eventRetention configuration.EventRetentionPolicy) *RedisEventRepository {
	return &RedisEventRepository{db: db, eventRetention: eventRetention}
}

func (repo *RedisEventRepository) ReportEvent(message *api.EventMessage) error {
	return repo.ReportEvents([]*api.EventMessage{message})
}

func (repo *RedisEventRepository) ReportEvents(messages []*api.EventMessage) error {
	fmt.Println("Reporting events", len(messages))
	if len(messages) == 0 {
		return nil
	}

	type eventData struct {
		key  string
		data []byte
	}
	data := []eventData{}
	uniqueJobSets := make(map[string]bool)

	for _, m := range messages {
		event, e := api.UnwrapEvent(m)
		if e != nil {
			return e
		}
		messageData, e := proto.Marshal(m)
		if e != nil {
			return e
		}
		key := getJobSetEventsKey(event.GetQueue(), event.GetJobSetId())
		data = append(data, eventData{key: key, data: messageData})
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
		for key, _ := range uniqueJobSets {
			pipe.Expire(key, repo.eventRetention.RetentionDuration)
		}
	}

	_, e := pipe.Exec()
	return e
}

func (repo *RedisEventRepository) ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {

	if lastId == "" {
		lastId = "0"
	}

	cmd, e := repo.db.XRead(&redis.XReadArgs{
		Streams: []string{getJobSetEventsKey(queue, jobSetId), lastId},
		Count:   limit,
		Block:   block,
	}).Result()

	// redis signals empty list by Nil
	if e == redis.Nil {
		return make([]*api.EventStreamMessage, 0), nil
	}

	if e != nil {
		return nil, e
	}

	messages := make([]*api.EventStreamMessage, 0)
	for _, m := range cmd[0].Messages {
		data := m.Values[dataKey]
		msg := &api.EventMessage{}
		bytes := []byte(data.(string))
		e = proto.Unmarshal(bytes, msg)
		if e != nil {
			return nil, e
		}
		messages = append(messages, &api.EventStreamMessage{Id: m.ID, Message: msg})
	}
	return messages, nil
}

func (repo *RedisEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	msg, err := repo.db.XRevRangeN(getJobSetEventsKey(queue, jobSetId), "+", "-", 1).Result()
	if err != nil {
		return "", err
	}
	if len(msg) > 0 {
		return msg[0].ID, nil
	}
	return "0", nil
}

func getJobSetEventsKey(queue, jobSetId string) string {
	return eventStreamPrefix + queue + ":" + jobSetId
}
