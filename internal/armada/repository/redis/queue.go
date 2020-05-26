package redis

import (
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
)

const queueHashKey = "Queue"

type RedisQueueRepository struct {
	db redis.UniversalClient
}

func NewRedisQueueRepository(db redis.UniversalClient) *RedisQueueRepository {
	return &RedisQueueRepository{db: db}
}

func (r *RedisQueueRepository) GetAllQueues() ([]*api.Queue, error) {
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

func (r *RedisQueueRepository) GetQueue(name string) (*api.Queue, error) {
	result, err := r.db.HGet(queueHashKey, name).Result()
	if err != nil {
		return nil, err
	}
	queue := &api.Queue{}
	e := proto.Unmarshal([]byte(result), queue)
	if e != nil {
		return nil, e
	}
	return queue, nil
}

func (r *RedisQueueRepository) CreateQueue(queue *api.Queue) error {

	data, e := proto.Marshal(queue)
	if e != nil {
		return e
	}
	result := r.db.HSet(queueHashKey, queue.Name, data)
	return result.Err()
}
