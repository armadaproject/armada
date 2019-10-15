package repository

import (
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/k8s-batch/internal/armada/api"
)

const queueHashKey = "Queue"

type QueueRepository interface {
	GetQueues() ([]*api.Queue, error)
	CreateQueue(queue *api.Queue) error
}

type RedisQueueRepository struct {
	db redis.UniversalClient
}

func NewRedisQueueRepository(db redis.UniversalClient) *RedisQueueRepository {
	return &RedisQueueRepository{db: db}
}

func (r *RedisQueueRepository) GetQueues() ([]*api.Queue, error) {
	result, err := r.db.HGetAll(queueHashKey).Result()
	if err != nil {
		return nil, err
	}

	queues := make([]*api.Queue, 0)
	for _, v := range result {
		queue := &api.Queue{}
		e := proto.Unmarshal([]byte(v), queue)
		if e != nil {
			return nil, e
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

func (r *RedisQueueRepository) CreateQueue(queue *api.Queue) error {

	data, e := proto.Marshal(queue)
	if e != nil {
		return e
	}
	result := r.db.HSet(queueHashKey, queue.Name, data)
	return result.Err()
}
