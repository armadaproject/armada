package configuration

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/internal/armada/authorization"
)

type ArmadaConfig struct {
	GrpcPort               string
	PriorityHalfTime       time.Duration
	Redis                  redis.UniversalOptions
	EventsRedis            redis.UniversalOptions
	BasicAuth              BasicAuthenticationConfig
	OpenIdAuth             OpenIdAuthenticationConfig
	PermissionGroupMapping map[authorization.Permission][]string
}

type OpenIdAuthenticationConfig struct {
	ProviderUrl string
	GroupsClaim string
}

type BasicAuthenticationConfig struct {
	EnableAuthentication bool
	Users                map[string]authorization.UserInfo
}
