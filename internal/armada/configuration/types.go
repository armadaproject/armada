package configuration

import (
	"time"

	"github.com/go-redis/redis"
)

type ArmadaConfig struct {
	GrpcPort         string
	PriorityHalfTime time.Duration
	Redis            redis.UniversalOptions
	EventsRedis      redis.UniversalOptions
	Authentication   AuthenticationConfig
}

type AuthenticationConfig struct {
	EnableAuthentication bool
	Users                map[string]string
}
