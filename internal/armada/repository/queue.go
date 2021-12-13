package repository

import (
	"fmt"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
)

const queueHashKey = "Queue"

type ErrQueueNotFound struct {
	QueueName string
}

func (err *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("queue %s does not exist", err.QueueName)
}

type ErrQueueAlreadyExists struct {
	QueueName string
}

func (err *ErrQueueAlreadyExists) Error() string {
	return fmt.Sprintf("queue %s already exists", err.QueueName)
}

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
		return nil, fmt.Errorf("[RedisQueueRepository.GetAllQueues] database error: %s", err)
	}

	queues := make([]*api.Queue, 0)
	for _, v := range result {
		queue := &api.Queue{}
		err = proto.Unmarshal([]byte(v), queue)
		if err != nil {
			return nil, fmt.Errorf("[RedisQueueRepository.GetAllQueues] error unmarshalling queue: %s", err)
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

func (r *RedisQueueRepository) GetQueue(name string) (*api.Queue, error) {
	result, err := r.db.HGet(queueHashKey, name).Result()
	if err == redis.Nil {
		return nil, &ErrQueueNotFound{QueueName: name}
	} else if err != nil {
		return nil, fmt.Errorf("[RedisQueueRepository.GetQueue] database error: %s", err)
	}

	queue := &api.Queue{}
	err = proto.Unmarshal([]byte(result), queue)
	if err != nil {
		return nil, fmt.Errorf("[RedisQueueRepository.GetQueue] error unmarshalling queue: %s", err)
	}
	return queue, nil
}

func (r *RedisQueueRepository) CreateQueue(queue *api.Queue) error {
	data, err := proto.Marshal(queue)
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] error marshalling queue: %s", err)
	}

	// HSetNX sets a key-value pair if the key doesn't already exist.
	// If the key exists, this is a no-op, and result is false.
	result, err := r.db.HSetNX(queueHashKey, queue.Name, data).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] database error: %s", err)
	}
	if !result {
		return &ErrQueueAlreadyExists{QueueName: queue.Name}
	}

	return nil
}

// TODO If the queue to be updated is deleted between this method checking if the qeueue exists and
// making the update, the deleted queue is re-added to Redis. There's no "update if exists"
// operation in Redis, so we need to do this with a script.
func (r *RedisQueueRepository) UpdateQueue(queue *api.Queue) error {
	existsResult, err := r.db.HExists(queueHashKey, queue.Name).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error reading from database: %s", err)
	} else if !existsResult {
		return &ErrQueueNotFound{QueueName: queue.Name}
	}

	data, err := proto.Marshal(queue)
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error marshalling queue: %s", err)
	}

	result := r.db.HSet(queueHashKey, queue.Name, data)
	if err := result.Err(); err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error writing to database: %s", err)
	}

	return nil
}

func (r *RedisQueueRepository) DeleteQueue(name string) error {
	result := r.db.HDel(queueHashKey, name)
	if err := result.Err(); err != nil {
		return fmt.Errorf("[RedisQueueRepository.DeleteQueue] error deleting queue: %s", err)
	}
	return nil
}
