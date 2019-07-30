package repository

import "github.com/go-redis/redis"

type EventRepository struct {
	db *redis.Client
}
