package repository

import (
	"errors"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
)

const queueHashKey = "Queue"

var ErrQueueNotFound = errors.New("Queue does not exist")
var ErrQueueAlreadyExists = errors.New("Queue already exists")

type QueueRepository interface {
	GetAllQueues() ([]*api.Queue, error)
	GetQueue(name string) (*api.Queue, error)
	CreateQueue(queue *api.Queue) error
	UpdateQueue(queue *api.Queue) error
	DeleteQueue(name string) error
}

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
	if err == redis.Nil {
		return nil, ErrQueueNotFound
	} else if err != nil {
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
	data, err := proto.Marshal(queue)
	if err != nil {
		return err
	}

	result, err := r.db.HSetNX(queueHashKey, queue.Name, data).Result()
	if err != nil {
		return err
	}

	if !result {
		return ErrQueueAlreadyExists
	}

	return nil
}

func (r *RedisQueueRepository) UpdateQueue(queue *api.Queue) error {
	existsResult, err := r.db.HExists(queueHashKey, queue.Name).Result()

	if err != nil {
		return err
	} else if !existsResult {
		return ErrQueueNotFound
	}

	data, err := proto.Marshal(queue)
	if err != nil {
		return err
	}

	result := r.db.HSet(queueHashKey, queue.Name, data)
	return result.Err()
}

func (r *RedisQueueRepository) DeleteQueue(name string) error {
	result := r.db.HDel(queueHashKey, name)
	return result.Err()
}
