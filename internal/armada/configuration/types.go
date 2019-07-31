package configuration

import "time"

type ArmadaConfig struct {
	GrpcPort         string
	PriorityHalfTime time.Duration
	Redis            RedisConfig
	EventsRedis      RedisConfig
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}
