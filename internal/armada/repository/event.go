package repository

import (
	"fmt"
	log "github.com/sirupsen/logrus"
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
	CheckStreamExists(queue string, jobSetId string) (bool, error)
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
		key := getJobSetEventsKey(event.GetQueue(), event.GetJobSetId())
		nullOutQueueAndJobset(m)
		messageData, e := proto.Marshal(m)
		if e != nil {
			return e
		}
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
		for key := range uniqueJobSets {
			pipe.Expire(key, repo.eventRetention.RetentionDuration)
		}
	}

	_, e := pipe.Exec()
	return e
}

func (repo *RedisEventRepository) CheckStreamExists(queue string, jobSetId string) (bool, error) {
	result, err := repo.db.Exists(getJobSetEventsKey(queue, jobSetId)).Result()
	if err != nil {
		return false, err
	}
	exists := result > 0
	return exists, nil
}

func (repo *RedisEventRepository) ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {

	if lastId == "" {
		lastId = "0"
	}

	cmd, err := repo.db.XRead(&redis.XReadArgs{
		Streams: []string{getJobSetEventsKey(queue, jobSetId), lastId},
		Count:   limit,
		Block:   block,
	}).Result()

	// redis signals empty list by Nil
	if err == redis.Nil {
		return make([]*api.EventStreamMessage, 0), nil
	} else if err != nil {
		return nil, fmt.Errorf("[RedisEventRepository.ReadEvents] error reading from database: %s", err)
	}

	messages := make([]*api.EventStreamMessage, 0)
	for _, m := range cmd[0].Messages {
		data := m.Values[dataKey]
		msg := &api.EventMessage{}
		bytes := []byte(data.(string))
		err = proto.Unmarshal(bytes, msg)
		if err != nil {
			return nil, fmt.Errorf("[RedisEventRepository.ReadEvents] error unmarshalling: %s", err)
		}
		populateQueueAndJobset(msg, queue, jobSetId)
		messages = append(messages, &api.EventStreamMessage{Id: m.ID, Message: msg})
	}
	return messages, nil
}

func (repo *RedisEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	msg, err := repo.db.XRevRangeN(getJobSetEventsKey(queue, jobSetId), "+", "-", 1).Result()
	if err != nil {
		return "", fmt.Errorf("[RedisEventRepository.GetLastMessageId] error reading from database: %s", err)
	}
	if len(msg) > 0 {
		return msg[0].ID, nil
	}
	return "0", nil
}

func getJobSetEventsKey(queue, jobSetId string) string {
	return eventStreamPrefix + queue + ":" + jobSetId
}

func nullOutQueueAndJobset(msg *api.EventMessage) {
	populateQueueAndJobset(msg, "", "")
}

func populateQueueAndJobset(msg *api.EventMessage, queue, jobSetId string) {
	switch event := msg.Events.(type) {
	case *api.EventMessage_Submitted:
		event.Submitted.Queue = queue
		event.Submitted.JobSetId = jobSetId
	case *api.EventMessage_Queued:
		event.Queued.Queue = queue
		event.Queued.JobSetId = jobSetId
	case *api.EventMessage_DuplicateFound:
		event.DuplicateFound.Queue = queue
		event.DuplicateFound.JobSetId = jobSetId
	case *api.EventMessage_Leased:
		event.Leased.Queue = queue
		event.Leased.JobSetId = jobSetId
	case *api.EventMessage_LeaseReturned:
		event.LeaseReturned.Queue = queue
		event.LeaseReturned.JobSetId = jobSetId
	case *api.EventMessage_LeaseExpired:
		event.LeaseExpired.Queue = queue
		event.LeaseExpired.JobSetId = jobSetId
	case *api.EventMessage_Pending:
		event.Pending.Queue = queue
		event.Pending.JobSetId = jobSetId
	case *api.EventMessage_Running:
		event.Running.Queue = queue
		event.Running.JobSetId = jobSetId
	case *api.EventMessage_UnableToSchedule:
		event.UnableToSchedule.Queue = queue
		event.UnableToSchedule.JobSetId = jobSetId
	case *api.EventMessage_Failed:
		event.Failed.Queue = queue
		event.Failed.JobSetId = jobSetId
	case *api.EventMessage_Succeeded:
		event.Succeeded.Queue = queue
		event.Succeeded.JobSetId = jobSetId
	case *api.EventMessage_Reprioritizing:
		event.Reprioritizing.Queue = queue
		event.Reprioritizing.JobSetId = jobSetId
	case *api.EventMessage_Reprioritized:
		event.Reprioritized.Queue = queue
		event.Reprioritized.JobSetId = jobSetId
	case *api.EventMessage_Cancelling:
		event.Cancelling.Queue = queue
		event.Cancelling.JobSetId = jobSetId
	case *api.EventMessage_Cancelled:
		event.Cancelled.Queue = queue
		event.Cancelled.JobSetId = jobSetId
	case *api.EventMessage_Terminated:
		event.Terminated.Queue = queue
		event.Terminated.JobSetId = jobSetId
	case *api.EventMessage_Utilisation:
		event.Utilisation.Queue = queue
		event.Utilisation.JobSetId = jobSetId
	case *api.EventMessage_IngressInfo:
		event.IngressInfo.Queue = queue
		event.IngressInfo.JobSetId = jobSetId
	case *api.EventMessage_Updated:
		event.Updated.Queue = queue
		event.Updated.JobSetId = jobSetId
	default:
		log.Warnf("Unknown message type %T, message queue and jobset will not be filled in", event)
	}
}
