package repository

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/go-redis/redis"
)

type Queue struct {
}

type QueueRepository interface {
	GetQueues() ([]*api.Queue, error)
	CreateQueue(queue *api.Queue) error
}

type RedisQueueRepository struct {
	db *redis.Client
}

func NewRedisQueueRepository(db *redis.Client) *RedisQueueRepository {
	return &RedisQueueRepository{db: db}
}

func (RedisQueueRepository) GetQueues() ([]*api.Queue, error) {
	panic("implement me")
}

func (RedisQueueRepository) CreateQueue(queue *api.Queue) error {
	panic("implement me")
}
