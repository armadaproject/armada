package configuration

import "time"

type ArmadaConfig struct {
	GrpcPort         string
	Redis            RedisConfig
	PriorityHalfTime time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}
