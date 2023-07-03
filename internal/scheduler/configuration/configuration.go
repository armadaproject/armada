package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	"github.com/armadaproject/armada/internal/common/config"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
)

const (
	// IsEvictedAnnotation is set on evicted jobs; the scheduler uses it to differentiate between
	// already-running and queued jobs.
	IsEvictedAnnotation = "armadaproject.io/isEvicted"
	// NodeIdLabel maps to a unique id associated with each node.
	// This label is automatically added to nodes within the NodeDb.
	NodeIdLabel = "armadaproject.io/nodeId"
	// ClusterIdLabel indicates which cluster each node belongs to.
	// This label is automatically added to nodes within the NodeDb.
	ClusterIdLabel = "armadaproject.io/clusterId"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Redis Comnfig
	Redis config.RedisConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Configuration controlling leader election
	Leader LeaderConfig
	// Configuration controlling metrics
	Metrics configuration.MetricsConfig
	// Scheduler configuration (this is shared with the old scheduler)
	Scheduling configuration.SchedulingConfig
	Auth       authconfig.AuthConfig
	Grpc       grpcconfig.GrpcConfig
	// Maximum number of strings that should be cached at any one time
	InternedStringsCacheSize uint32 `validate:"required"`
	// How often the scheduling cycle should run
	CyclePeriod time.Duration `validate:"required"`
	// How often the job scheduling should run
	// This is expected to be a greater value than CyclePeriod as we don't need to schedule every cycle
	// This keeps the system more responsive as other operations happen in each cycle - such as state changes
	SchedulePeriod time.Duration `validate:"required"`
	// The maximum time allowed for a job scheduling round
	MaxSchedulingDuration time.Duration `validate:"required"`
	// How long after a heartbeat an executor will be considered lost
	ExecutorTimeout time.Duration `validate:"required"`
	// Maximum number of rows to fetch in a given query
	DatabaseFetchSize int `validate:"required"`
	// Timeout to use when sending messages to pulsar
	PulsarSendTimeout time.Duration `validate:"required"`
}

type LeaderConfig struct {
	// Valid modes are "standalone" or "kubernetes"
	Mode string `validate:"required"`
	// Name of the K8s Lock Object
	LeaseLockName string
	// Namespace of the K8s Lock Object
	LeaseLockNamespace string
	// The name of the pod
	PodName string
	// How long the lease is held for.
	// Non leaders much wait this long before trying to acquire the lease
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting leader will retry refreshing leadership before giving up.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should waite between tries of actions.
	RetryPeriod time.Duration
}
