package repository

import (
	"fmt"

	"github.com/go-redis/redis"
)

type RedisHealth struct {
	db redis.UniversalClient
}

func NewRedisHealth(db redis.UniversalClient) *RedisHealth {
	return &RedisHealth{db: db}
}

func (r *RedisHealth) Check() error {
	_, err := r.db.Ping().Result()
	if err == nil {
		return nil
	} else {
		return fmt.Errorf("[RedisHealth.Check] error: %s", err)
	}
}
