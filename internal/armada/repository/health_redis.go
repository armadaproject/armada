package repository

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisHealth struct {
	db redis.UniversalClient
}

func NewRedisHealth(db redis.UniversalClient) *RedisHealth {
	return &RedisHealth{db: db}
}

func (r *RedisHealth) Check() error {
	_, err := r.db.Ping(context.Background()).Result()
	if err == nil {
		return nil
	} else {
		return fmt.Errorf("[RedisHealth.Check] error: %s", err)
	}
}
