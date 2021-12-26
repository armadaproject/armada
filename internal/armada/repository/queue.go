package repository

import (
	"errors"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

const queueHashKey = "Queue"

// TODO These errors should be of a specific error type, e.g.:
// type ErrQueueNotFound struct {
// 	queueName string
// }
// func (err *ErrQueueNotFound) Error() {
// 	return fmt.Sprintf("Queue %s does not exist", err.queueName)
// }
// In doing so, if we wrap errors as they travel up the call stack
// (using fmt.Errorf with the %w verb, i.e., fmt.Errorf("err %w", err))
// and easily detect at the gRPC handler what the underlying issue is.
// We need this since we need to create errors in the gRPC handler with the correct gRPC error code.
// For more on error wrapping, see:
// https://go.dev/blog/go1.13-errors

var ErrQueueNotFound = errors.New("Queue does not exist")
var ErrQueueAlreadyExists = errors.New("Queue already exists")

type QueueRepository interface {
	GetAllQueues() (queue.Queues, error)
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

func (r *RedisQueueRepository) GetAllQueues() (queue.Queues, error) {
	result, err := r.db.HGetAll(queueHashKey).Result()
	if err != nil {
		return nil, err
	}

	queues := make([]queue.Queue, 0)
	for _, v := range result {
		apiQueue := &api.Queue{}
		e := proto.Unmarshal([]byte(v), apiQueue)
		if e != nil {
			return nil, e
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
		return queue.Queue{}, ErrQueueNotFound
	} else if err != nil {
		return queue.Queue{}, err
	}

	apiQueue := &api.Queue{}
	e := proto.Unmarshal([]byte(result), apiQueue)
	if e != nil {
		return queue.Queue{}, e
	}

	return queue.NewQueue(apiQueue)
}

func (r *RedisQueueRepository) CreateQueue(queue queue.Queue) error {
	data, err := proto.Marshal(queue.ToAPI())
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

func (r *RedisQueueRepository) UpdateQueue(queue queue.Queue) error {
	existsResult, err := r.db.HExists(queueHashKey, queue.Name).Result()

	if err != nil {
		return err
	} else if !existsResult {
		return ErrQueueNotFound
	}

	data, err := proto.Marshal(queue.ToAPI())
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
