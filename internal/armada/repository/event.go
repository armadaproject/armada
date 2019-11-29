package repository

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/internal/armada/api"
)

const eventStreamPrefix = "Events:"
const dataKey = "message"

type EventRepository interface {
	ReportEvent(message *api.EventMessage) error
	ReportEvents(message []*api.EventMessage) error
	ReadEvents(jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
	GetLastMessageId(jobSetId string) (string, error)
}

type RedisEventRepository struct {
	db redis.UniversalClient
}

func NewRedisEventRepository(db redis.UniversalClient) *RedisEventRepository {
	return &RedisEventRepository{db: db}
}

func (repo *RedisEventRepository) ReportEvent(message *api.EventMessage) error {
	return repo.ReportEvents([]*api.EventMessage{message})
}

func (repo *RedisEventRepository) ReportEvents(message []*api.EventMessage) error {

	type eventData struct {
		jobSetId string
		data     []byte
	}
	data := []eventData{}

	for _, m := range message {
		event, e := api.UnwrapEvent(m)
		if e != nil {
			return e
		}
		messageData, e := proto.Marshal(m)
		if e != nil {
			return e
		}
		data = append(data, eventData{jobSetId: event.GetJobSetId(), data: messageData})
	}

	pipe := repo.db.Pipeline()
	for _, e := range data {
		pipe.XAdd(&redis.XAddArgs{
			Stream: eventStreamPrefix + e.jobSetId,
			Values: map[string]interface{}{
				dataKey: e.data,
			},
		})
	}
	_, e := pipe.Exec()
	return e
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

func (repo *RedisEventRepository) GetLastMessageId(jobSetId string) (string, error) {
	msg, err := repo.db.XRevRangeN(eventStreamPrefix+jobSetId, "+", "-", 1).Result()
	if err != nil {
		return "", err
	}
	if len(msg) > 0 {
		return msg[0].ID, nil
	}
	return "0", nil
}
