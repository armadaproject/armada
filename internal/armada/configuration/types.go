package configuration

import (
	"time"

	"github.com/go-redis/redis"
)

type ArmadaConfig struct {
	GrpcPort         uint16
	MetricsPort      uint16
	PriorityHalfTime time.Duration
	Redis            redis.UniversalOptions
	EventsRedis      redis.UniversalOptions
	BasicAuth        AuthenticationConfig
	OpenIdAuth       OpenIdAuthenticationConfig
}

type OpenIdAuthenticationConfig struct {
	ProviderUrl string
	ClientId    string
	GroupsClaim string
}

type AuthenticationConfig struct {
	EnableAuthentication bool
	Users                map[string]string
}
