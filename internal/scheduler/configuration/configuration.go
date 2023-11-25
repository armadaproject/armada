package configuration

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	authconfig "github.com/armadaproject/armada/internal/common/auth/configuration"
	"github.com/armadaproject/armada/internal/common/config"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	// NodeIdLabel maps to a unique id associated with each node.
	// This label is automatically added to nodes within the NodeDb.
	NodeIdLabel = "armadaproject.io/nodeId"
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
	// Configuration for new scheduler metrics.
	// Due to replace metrics configured via the above entry.
	SchedulerMetrics MetricsConfig
	// Scheduler configuration (this is shared with the old scheduler)
	Scheduling configuration.SchedulingConfig
	Auth       authconfig.AuthConfig
	Grpc       grpcconfig.GrpcConfig
	Http       HttpConfig
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	PprofPort *uint16
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

type MetricsConfig struct {
	// If true, disable metric collection and publishing.
	Disabled bool
	// The scheduler exports metrics tracking job failures.
	// These metrics may be annotated by flags indicating the type of error.
	// For example, if TrackedErrorRegexes contains the following entry,
	// "isCudaError": "/CUDA/"
	// then job failure metrics will have a label with value
	TrackedErrorRegexByLabel map[string]string
	// Metrics are exported for these resources.
	TrackedResourceNames []v1.ResourceName
	// Controls the cycle time metrics.
	CycleTimeConfig PrometheusSummaryConfig
}

// PrometheusSummaryConfig contains the relevant config for a prometheus.Summary.
type PrometheusSummaryConfig struct {
	// Objectives defines the quantile rank estimates with their respective
	// absolute error. If Objectives[q] = e, then the value reported for q
	// will be the φ-quantile value for some φ between q-e and q+e.  The
	// default value is an empty map, resulting in a summary without
	// quantiles.
	Objectives map[float64]float64

	// MaxAge defines the duration for which an observation stays relevant
	// for the summary. Only applies to pre-calculated quantiles, does not
	// apply to _sum and _count. Must be positive. The default value is
	// DefMaxAge.
	MaxAge time.Duration
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
	// Connection details to the leader
	LeaderConnection client.ApiConnectionDetails
}

type HttpConfig struct {
	Port int `validate:"required"`
}
