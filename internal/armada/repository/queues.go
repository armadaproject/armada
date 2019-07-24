package repository

import (
	"github.com/go-redis/redis"
)

type Queue struct {
}

type QueueRepository interface {
	GetQueues() ([]string, error)
}

type RedisQueueRepository struct {
	Db *redis.Client
}

func (RedisQueueRepository) GetQueues() ([]string, error) {
	panic("implement me")
}
