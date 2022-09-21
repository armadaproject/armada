package repository

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	pool "github.com/jolestar/go-commons-pool"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/pkg/api"
)

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}

type LegacyRedisEventRepository struct {
	db               redis.UniversalClient
	eventRetention   configuration.EventRetentionPolicy
	compressorPool   *pool.ObjectPool
	decompressorPool *pool.ObjectPool
}

func NewLegacyRedisEventRepository(db redis.UniversalClient, eventRetention configuration.EventRetentionPolicy) *LegacyRedisEventRepository {
	// This is basically the default config but with a max of 100 rather than 8 and a min of 10 rather than 0.
	poolConfig := pool.ObjectPoolConfig{
		MaxTotal:                 100,
		MaxIdle:                  50,
		MinIdle:                  10,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     30 * time.Minute,
		SoftMinEvictableIdleTime: math.MaxInt64,
		TimeBetweenEvictionRuns:  0,
		NumTestsPerEvictionRun:   10,
	}

	compressorPool := pool.NewObjectPool(context.Background(), pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return compress.NewZlibCompressor(1024)
		}), &poolConfig)

	decompressorPool := pool.NewObjectPool(context.Background(), pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return compress.NewZlibDecompressor()
		}), &poolConfig)
	return &LegacyRedisEventRepository{db: db, eventRetention: eventRetention, compressorPool: compressorPool, decompressorPool: decompressorPool}
}

// ReportEvent reports the event to redis.  Note that this function may modify the supplied message in-place
func (repo *LegacyRedisEventRepository) ReportEvent(message *api.EventMessage) error {
	return repo.ReportEvents([]*api.EventMessage{message})
}

// ReportEvents reports events to redis.  Note that this function may modify the supplied messages in-place
func (repo *LegacyRedisEventRepository) ReportEvents(messages []*api.EventMessage) error {
	if len(messages) == 0 {
		return nil
	}

	compressor, err := repo.compressorPool.BorrowObject(context.Background())
	if err != nil {
		return err
	}
	defer func(compressorPool *pool.ObjectPool, ctx context.Context, object interface{}) {
		err := compressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning compressor to pool")
		}
	}(repo.compressorPool, context.Background(), compressor)

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
		m, e = compressEventIfNecessary(m, compressor.(compress.Compressor))
		if e != nil {
			return e
		}
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

func (repo *LegacyRedisEventRepository) CheckStreamExists(queue string, jobSetId string) (bool, error) {
	result, err := repo.db.Exists(getJobSetEventsKey(queue, jobSetId)).Result()
	if err != nil {
		return false, err
	}
	exists := result > 0
	return exists, nil
}

func (repo *LegacyRedisEventRepository) ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
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
		return nil, fmt.Errorf("[LegacyRedisEventRepository.ReadEvents] error reading from database: %s", err)
	}

	decompressor, err := repo.decompressorPool.BorrowObject(context.Background())
	if err != nil {
		log.WithError(err).Errorf("Error borrowing decompressor")
	}

	defer func(decompressorPool *pool.ObjectPool, ctx context.Context, object interface{}) {
		err := decompressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning decompressorPool to pool")
		}
	}(repo.decompressorPool, context.Background(), decompressor)

	messages := make([]*api.EventStreamMessage, 0)
	for _, m := range cmd[0].Messages {
		data := m.Values[dataKey]
		msg := &api.EventMessage{}
		bytes := []byte(data.(string))
		err = proto.Unmarshal(bytes, msg)
		if err != nil {
			return nil, fmt.Errorf("[LegacyRedisEventRepository.ReadEvents] error unmarshalling: %s", err)
		}
		msg, err = DecompressEventIfNecessary(msg, decompressor.(compress.Decompressor))
		if err != nil {
			return nil, fmt.Errorf("[LegacyRedisEventRepository.ReadEvents] error decompressing: %s", err)
		}
		populateQueueAndJobset(msg, queue, jobSetId)
		messages = append(messages, &api.EventStreamMessage{Id: m.ID, Message: msg})
	}
	return messages, nil
}

func (repo *LegacyRedisEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	msg, err := repo.db.XRevRangeN(getJobSetEventsKey(queue, jobSetId), "+", "-", 1).Result()
	if err != nil {
		return "", fmt.Errorf("[LegacyRedisEventRepository.GetLastMessageId] error reading from database: %s", err)
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

func compressEventIfNecessary(msg *api.EventMessage, compressor compress.Compressor) (*api.EventMessage, error) {
	if msg.GetFailed() != nil {
		messageData, e := proto.Marshal(msg)
		if e != nil {
			return nil, e
		}
		compressedBytes, e := compressor.Compress(messageData)
		if e != nil {
			return nil, e
		}
		return &api.EventMessage{
			Events: &api.EventMessage_FailedCompressed{
				FailedCompressed: &api.JobFailedEventCompressed{
					Event: compressedBytes,
				},
			},
		}, nil
	}
	return msg, nil
}

func DecompressEventIfNecessary(msg *api.EventMessage, decompressor compress.Decompressor) (*api.EventMessage, error) {
	failed := msg.GetFailedCompressed()
	if failed != nil {
		decompressedBytes, err := decompressor.Decompress(failed.GetEvent())
		if err != nil {
			return nil, err
		}
		msg := &api.EventMessage{}
		err = proto.Unmarshal(decompressedBytes, msg)
		if err != nil {
			return nil, err
		}
		return msg, nil
	}
	return msg, nil
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
	case *api.EventMessage_Preempted:
		event.Preempted.Queue = queue
		event.Preempted.JobSetId = jobSetId
	default:
		log.Warnf("Unknown message type %T, message queue and jobset will not be filled in", event)
	}
}
