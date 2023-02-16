package scheduler

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	"github.com/armadaproject/armada/internal/common/config"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
)

const (
	// TargetNodeIdAnnotation if set on a pod, the value of this annotation is interpreted as the id of a node
	// and only the node with that id will be considered for scheduling the pod.
	TargetNodeIdAnnotation = "armadaproject.io/targetNodeId"
	// IsEvictedAnnotation, indicates a pod was evicted in this round and is currently running.
	// Used by the scheduler to differentiate between pods from running and queued jobs.
	IsEvictedAnnotation = "armadaproject.io/isEvicted"
	// JobIdAnnotation if set on a pod, indicates which job this pod is part of.
	JobIdAnnotation = "armadaproject.io/jobId"
	// QueueAnnotation if set on a pod, indicates which queue this pod is part of.
	QueueAnnotation = "armadaproject.io/queue"
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
	// Scheduler configuration (this is shared with the old scheduler)
	Scheduling configuration.SchedulingConfig
	Auth       authconfig.AuthConfig
	Grpc       grpcconfig.GrpcConfig
	// Maximum number of strings that should be cached at any one time
	InternedStringsCacheSize uint32 `validate:"required"`
	// How often the scheduling cycle should run
	CyclePeriod time.Duration `validate:"required"`
	// Maximum number of rows to fetch in a given query
	DatabaseFetchSize int `validate:"required"`
	// Timeout to use when sending messages to pulsar
	PulsarSendTimeout time.Duration `validate:"required"`
}

type LeaderConfig struct {
	// Valid modes are "standalone" or "cluster"
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
