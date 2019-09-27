package repository

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"time"
)

const eventStreamPrefix = "Events:"
const dataKey = "message"

type EventRepository interface {
	ReportEvent(message *api.EventMessage) error
	ReadEvents(jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
}

type RedisEventRepository struct {
	db redis.UniversalClient
}

func NewRedisEventRepository(db redis.UniversalClient) *RedisEventRepository {
	return &RedisEventRepository{db: db}
}

func (repo *RedisEventRepository) ReportEvent(message *api.EventMessage) error {
	event, e := api.UnwrapEvent(message)
	if e != nil {
		return e
	}

	messageData, e := proto.Marshal(message)
	if e != nil {
		return e
	}

	result := repo.db.XAdd(&redis.XAddArgs{
		Stream: eventStreamPrefix + event.GetJobSetId(),
		Values: map[string]interface{}{
			dataKey: messageData,
		},
	})
	return result.Err()
}

func (repo *RedisEventRepository) ReadEvents(jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {

	if lastId == "" {
		lastId = "0"
	}

	cmd, e := repo.db.XRead(&redis.XReadArgs{
		Streams: []string{eventStreamPrefix + jobSetId, lastId},
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
