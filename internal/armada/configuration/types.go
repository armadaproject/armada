package configuration

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/configuration"
)

type ArmadaConfig struct {
	Auth configuration.AuthConfig

	GrpcPort           uint16
	HttpPort           uint16
	MetricsPort        uint16
	CorsAllowedOrigins []string
	PriorityHalfTime   time.Duration
	Redis              redis.UniversalOptions
	EventsKafka        KafkaConfig
	EventsNats         NatsConfig
	EventsRedis        redis.UniversalOptions

	Scheduling      SchedulingConfig
	QueueManagement QueueManagementConfig
	EventRetention  EventRetentionPolicy
	Metrics         MetricsConfig
}

type SchedulingConfig struct {
	UseProbabilisticSchedulingForAllResources bool
	QueueLeaseBatchSize                       uint
	MinimumResourceToSchedule                 common.ComputeResourcesFloat
	MaximalClusterFractionToSchedule          map[string]float64
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	MaximalResourceFractionPerQueue           map[string]float64
	Lease                                     LeaseSettings
	DefaultJobLimits                          common.ComputeResources
	MaxRetries                                uint // Maximum number of retries before a Job is failed
	ResourceScarcity                          map[string]float64
	PoolResourceScarcity                      map[string]map[string]float64
}

type EventRetentionPolicy struct {
	ExpiryEnabled     bool
	RetentionDuration time.Duration
}

type LeaseSettings struct {
	ExpireAfter        time.Duration
	ExpiryLoopInterval time.Duration
}

type KafkaConfig struct {
	Brokers                  []string
	Topic                    string
	ConsumerGroupID          string
	JobStatusConsumerGroupID string
}

type NatsConfig struct {
	Servers        []string
	ClusterID      string
	Subject        string
	QueueGroup     string
	JobStatusGroup string
}

type QueueManagementConfig struct {
	AutoCreateQueues      bool
	DefaultPriorityFactor float64
}

type MetricsConfig struct {
	RefreshInterval time.Duration
}
