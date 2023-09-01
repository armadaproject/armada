package repository

import (
	gocontext "context"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository/apimessages"
	"github.com/armadaproject/armada/internal/armada/repository/sequence"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	eventStreamPrefix = "Events:"
	dataKey           = "message"
)

type EventRepository interface {
	CheckStreamExists(queue string, jobSetId string) (bool, error)
	ReadEvents(queue, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error)
	GetLastMessageId(queue, jobSetId string) (string, error)
}

type RedisEventRepository struct {
	db               redis.UniversalClient
	decompressorPool *pool.ObjectPool
}

func NewEventRepository(db redis.UniversalClient) *RedisEventRepository {
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

	decompressorPool := pool.NewObjectPool(armadacontext.Background(), pool.NewPooledObjectFactorySimple(
		func(ctx gocontext.Context) (interface{}, error) {
			return compress.NewZlibDecompressor(), nil
		}), &poolConfig)

	return &RedisEventRepository{db: db, decompressorPool: decompressorPool}
}

func (repo *RedisEventRepository) CheckStreamExists(queue string, jobSetId string) (bool, error) {
	result, err := repo.db.Exists(getJobSetEventsKey(queue, jobSetId)).Result()
	if err != nil {
		return false, err
	}
	exists := result > 0
	return exists, nil
}

func (repo *RedisEventRepository) ReadEvents(queue string, jobSetId string, lastId string, limit int64, block time.Duration) ([]*api.EventStreamMessage, error) {
	from, err := sequence.Parse(lastId)
	if err != nil {
		return nil, err
	}
	seqId := from.PrevRedisId()
	cmd, err := repo.db.XRead(&redis.XReadArgs{
		Streams: []string{getJobSetEventsKey(queue, jobSetId), seqId},
		Count:   limit,
		Block:   block,
	}).Result()

	// redis signals empty list by Nil
	if err == redis.Nil {
		return make([]*api.EventStreamMessage, 0), nil
	} else if err != nil {
		return nil, errors.WithStack(fmt.Errorf("%s (fromId: %s, seqId: %s)", err, from, seqId))
	}

	messages := make([]*api.EventStreamMessage, 0, len(cmd[0].Messages))
	for _, m := range cmd[0].Messages {
		// TODO: here we decompress all the events we fetched from the db- it would be much better
		// If we could decompress lazily, but the interface confines us somewhat here
		apiEvents, err := repo.extractEvents(m, queue, jobSetId)
		if err != nil {
			return nil, err
		}
		for i, msg := range apiEvents {
			msgId, err := sequence.FromRedisId(m.ID, i, i == len(apiEvents)-1)
			if err != nil {
				return nil, err
			}
			if msgId.IsAfter(from) {
				messages = append(messages, &api.EventStreamMessage{Id: msgId.String(), Message: msg})
			}
		}
	}
	return messages, nil
}

func (repo *RedisEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	msg, err := repo.db.XRevRangeN(getJobSetEventsKey(queue, jobSetId), "+", "-", 1).Result()
	if err != nil {
		return "", errors.Wrap(err, "Error retrieving the last message id from Redis")
	}
	if len(msg) > 0 {
		apiEvents, err := repo.extractEvents(msg[0], queue, jobSetId)
		if err != nil {
			return "", err
		}
		msgId, err := sequence.FromRedisId(msg[0].ID, len(apiEvents)-1, true)
		if err != nil {
			return "", err
		}
		return msgId.String(), nil
	}
	return "0", nil
}

func (repo *RedisEventRepository) extractEvents(msg redis.XMessage, queue, jobSetId string) ([]*api.EventMessage, error) {
	data := msg.Values[dataKey]
	bytes := []byte(data.(string))
	decompressor, err := repo.decompressorPool.BorrowObject(armadacontext.Background())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func(decompressorPool *pool.ObjectPool, ctx *armadacontext.ArmadaContext, object interface{}) {
		err := decompressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning decompressor to pool")
		}
	}(repo.decompressorPool, armadacontext.Background(), decompressor)
	decompressedData, err := decompressor.(compress.Decompressor).Decompress(bytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	es := &armadaevents.EventSequence{}
	err = proto.Unmarshal(decompressedData, es)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// These fields are not present in the db messages, so we add them back here
	es.Queue = queue
	es.JobSetName = jobSetId
	return apimessages.FromEventSequence(es)
}

func getJobSetEventsKey(queue, jobSetId string) string {
	return eventStreamPrefix + queue + ":" + jobSetId
}
