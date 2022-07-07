package repository

import (
	"github.com/go-redis/redis"
	"fmt"
)

func HealthCheck(db redis.UniversalClient) error {
	_, err := db.Ping().Result()
	if err == nil {
		return nil
	} else {
		return fmt.Errorf("[RedisHealth.Check] error: %s", err)
	}
}
