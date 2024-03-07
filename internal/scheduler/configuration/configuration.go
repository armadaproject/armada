package configuration

import (
	"time"

	"github.com/go-playground/validator/v10"
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
	Metrics LegacyMetricsConfig
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

func (c Configuration) Validate() error {
	validate := validator.New()
	validate.RegisterStructValidation(configuration.SchedulingConfigValidation, configuration.SchedulingConfig{})
	return validate.Struct(c)
}

type MetricsConfig struct {
	// If true, disable metric collection and publishing.
	Disabled bool
	// Regexes used for job error categorisation.
	// Specifically, the subCategory label for job failure counters is the first regex that matches the job error.
	// If no regex matches, the subCategory label is the empty string.
	TrackedErrorRegexes []string
	// Metrics are exported for these resources.
	TrackedResourceNames []v1.ResourceName
	// Optionally rename resources in exported metrics.
	// E.g., if ResourceRenaming["nvidia.com/gpu"] = "gpu", then metrics for resource "nvidia.com/gpu" use resource name "gpu" instead.
	// This can be used to avoid illegal Prometheus metric names (e.g., for "nvidia.com/gpu" as "/" is not allowed).
	// Allowed characters in resource names are [a-zA-Z_:][a-zA-Z0-9_:]*
	// It can also be used to track multiple resources within the same metric, e.g., "nvidia.com/gpu" and "amd.com/gpu".
	ResourceRenaming map[v1.ResourceName]string
	// Controls the cycle time metrics.
	// TODO(albin): Not used yet.
	CycleTimeConfig PrometheusSummaryConfig
	// The first matching regex of each error message is cached in an LRU cache.
	// This setting controls the cache size.
	MatchedRegexIndexByErrorMessageCacheSize uint64
	// Reset metrics this often. Resetting periodically ensures inactive time series are garbage-collected.
	ResetInterval time.Duration
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

// TODO: ALl this needs to be unified with MetricsConfig
type LegacyMetricsConfig struct {
	Port            uint16
	RefreshInterval time.Duration
	Metrics         SchedulerMetricsConfig
}

type SchedulerMetricsConfig struct {
	ScheduleCycleTimeHistogramSettings  HistogramConfig
	ReconcileCycleTimeHistogramSettings HistogramConfig
}

type HistogramConfig struct {
	Start  float64
	Factor float64
	Count  int
}
