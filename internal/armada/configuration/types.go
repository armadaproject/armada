package configuration

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/common"
)

type UserInfo struct {
	Password string
	Groups   []string
}

type ArmadaConfig struct {
	AnonymousAuth bool

	GrpcPort               uint16
	MetricsPort            uint16
	PriorityHalfTime       time.Duration
	Redis                  redis.UniversalOptions
	EventsRedis            redis.UniversalOptions
	BasicAuth              BasicAuthenticationConfig
	OpenIdAuth             OpenIdAuthenticationConfig
	PermissionGroupMapping map[permissions.Permission][]string
	PermissionScopeMapping map[permissions.Permission][]string

	Scheduling SchedulingConfig
}

type OpenIdAuthenticationConfig struct {
	ProviderUrl string
	GroupsClaim string
}

type BasicAuthenticationConfig struct {
	Users map[string]UserInfo
}

type SchedulingConfig struct {
	UseProbabilisticSchedulingForAllResources bool
	QueueLeaseBatchSize                       uint
	MinimumResourceToSchedule                 common.ComputeResourcesFloat
	MaximalClusterFractionToSchedule          float64
}
