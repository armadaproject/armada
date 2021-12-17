package configuration

import (
	"time"

	"github.com/go-redis/redis"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/configuration"
)

type ArmadaConfig struct {
	Auth configuration.AuthConfig

	GrpcPort           uint16
	HttpPort           uint16
	MetricsPort        uint16
	CorsAllowedOrigins []string

	PriorityHalfTime    time.Duration
	CancelJobsBatchSize int
	Redis               redis.UniversalOptions
	Events              EventsConfig
	EventsNats          NatsConfig
	EventsJetstream     JetstreamConfig
	EventsRedis         redis.UniversalOptions

	Scheduling        SchedulingConfig
	QueueManagement   QueueManagementConfig
	DatabaseRetention DatabaseRetentionPolicy
	EventRetention    EventRetentionPolicy

	Metrics MetricsConfig
}

type SchedulingConfig struct {
	UseProbabilisticSchedulingForAllResources bool
	QueueLeaseBatchSize                       uint
	MinimumResourceToSchedule                 common.ComputeResourcesFloat
	MaximumLeasePayloadSizeBytes              int
	MaximalClusterFractionToSchedule          map[string]float64
	MaximalResourceFractionToSchedulePerQueue map[string]float64
	MaximalResourceFractionPerQueue           map[string]float64
	MaximumJobsToSchedule                     int
	Lease                                     LeaseSettings
	DefaultJobLimits                          common.ComputeResources
	DefaultJobTolerations                     []v1.Toleration
	MaxRetries                                uint // Maximum number of retries before a Job is failed
	ResourceScarcity                          map[string]float64
	PoolResourceScarcity                      map[string]map[string]float64
	MaxPodSpecSizeBytes                       uint
	MinJobResources                           v1.ResourceList
}

type DatabaseRetentionPolicy struct {
	JobRetentionDuration time.Duration
}

type EventRetentionPolicy struct {
	ExpiryEnabled     bool
	RetentionDuration time.Duration
}

type LeaseSettings struct {
	ExpireAfter        time.Duration
	ExpiryLoopInterval time.Duration
}

type EventsConfig struct {
	StoreQueue     string // Queue group for event storage processors
	JobStatusQueue string // Queue group for running job status processor

	ProcessorBatchSize             int           // Maximum event batch size
	ProcessorMaxTimeBetweenBatches time.Duration // Maximum time between batches
	ProcessorTimeout               time.Duration // Timeout for reporting event or stopping batcher before erroring out
}

type NatsConfig struct {
	Servers   []string
	ClusterID string
	Subject   string
	Timeout   time.Duration // Timeout for receiving a reply back from the stan server for PublishAsync
}

type JetstreamConfig struct {
	Servers     []string
	StreamName  string
	Replicas    int
	Subject     string
	MaxAgeDays  int
	ConnTimeout time.Duration
	InMemory    bool // Whether stream should be stored in memory (as opposed to on disk)
}

type QueueManagementConfig struct {
	AutoCreateQueues      bool
	DefaultPriorityFactor float64
}

type MetricsConfig struct {
	RefreshInterval time.Duration
}
