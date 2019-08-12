package configuration

import "time"

type ArmadaConfig struct {
	GrpcPort         string
	PriorityHalfTime time.Duration
	Redis            RedisConfig
	EventsRedis      RedisConfig
	Authentication   AuthenticationConfig
}

type RedisConfig struct {
	Addr     string
	Password string
	Db       int
}

type AuthenticationConfig struct {
	EnableAuthentication bool
	Users                map[string]string
}
