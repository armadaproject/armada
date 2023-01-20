package repository

import (
	"fmt"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

const queueHashKey = "Queue"

type ErrQueueNotFound struct {
	QueueName string
}

func (err *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("could not find queue %q", err.QueueName)
}

type ErrQueueAlreadyExists struct {
	QueueName string
}

func (err *ErrQueueAlreadyExists) Error() string {
	return fmt.Sprintf("queue %s already exists", err.QueueName)
}

type QueueRepository interface {
	GetAllQueues() ([]queue.Queue, error)
	GetQueue(name string) (queue.Queue, error)
	CreateQueue(queue.Queue) error
	UpdateQueue(queue.Queue) error
	DeleteQueue(name string) error
}

type RedisQueueRepository struct {
	db redis.UniversalClient
}

func NewRedisQueueRepository(db redis.UniversalClient) *RedisQueueRepository {
	return &RedisQueueRepository{db: db}
}

func (r *RedisQueueRepository) GetAllQueues() ([]queue.Queue, error) {
	result, err := r.db.HGetAll(queueHashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisQueueRepository.GetAllQueues] error reading from database: %s", err)
	}

	queues := make([]queue.Queue, 0)
	for _, v := range result {
		apiQueue := &api.Queue{}
		e := proto.Unmarshal([]byte(v), apiQueue)
		if e != nil {
			return nil, fmt.Errorf("[RedisQueueRepository.GetAllQueues] error unmarshalling queue: %s", err)
		}
		queue, err := queue.NewQueue(apiQueue)
		if err != nil {
			return nil, err
		}

		queues = append(queues, queue)
	}
	return queues, nil
}

func (r *RedisQueueRepository) GetQueue(name string) (queue.Queue, error) {
	result, err := r.db.HGet(queueHashKey, name).Result()
	if err == redis.Nil {
		return queue.Queue{}, &ErrQueueNotFound{QueueName: name}
	} else if err != nil {
		return queue.Queue{}, fmt.Errorf("[RedisQueueRepository.GetQueue] error reading from database: %s", err)
	}

	apiQueue := &api.Queue{}
	e := proto.Unmarshal([]byte(result), apiQueue)
	if e != nil {
		return queue.Queue{}, fmt.Errorf("[RedisQueueRepository.GetQueue] error unmarshalling queue: %s", err)
	}

	return queue.NewQueue(apiQueue)
}

func (r *RedisQueueRepository) CreateQueue(queue queue.Queue) error {
	data, err := proto.Marshal(queue.ToAPI())
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] error marshalling queue: %s", err)
	}

	// HSetNX sets a key-value pair if the key doesn't already exist.
	// If the key exists, this is a no-op, and result is false.
	result, err := r.db.HSetNX(queueHashKey, queue.Name, data).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.CreateQueue] error writing to database: %s", err)
	}
	if !result {
		return &ErrQueueAlreadyExists{QueueName: queue.Name}
	}

	return nil
}

// TODO If the queue to be updated is deleted between this method checking if the queue exists and
// making the update, the deleted queue is re-added to Redis. There's no "update if exists"
// operation in Redis, so we need to do this with a script or transaction.
func (r *RedisQueueRepository) UpdateQueue(queue queue.Queue) error {
	existsResult, err := r.db.HExists(queueHashKey, queue.Name).Result()
	if err != nil {
		return fmt.Errorf("[RedisQueueRepository.UpdateQueue] error reading from database: %s", err)
	} else if !existsResult {
		return &ErrQueueNotFound{QueueName: queue.Name}
	}

	data, err := proto.Marshal(queue.ToAPI())
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
