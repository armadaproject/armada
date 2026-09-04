package configuration

import (
	"time"

	"github.com/redis/go-redis/v9"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/observability"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/leaderelection"
)

type EventIngesterConfiguration struct {
	// Database configuration
	Redis redis.UniversalOptions
	// Database configuration - write to a second redis with this option
	// Nearly always not set, but can be useful if migrating the armada control plane to another kubernetes cluster
	RedisReplica redis.UniversalOptions
	// Metrics configuration
	MetricsPort uint16
	// Metrics configuration for Redis memory metrics collection
	Metrics MetricsConfig
	// Configuration controlling OpenTelemetry observability
	Observability observability.ObservabilityConfig
	// General Pulsar configuration
	Pulsar commonconfig.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which event message will be compressed when inserting in the database
	MinMessageCompressionSize int
	// Max size in bytes that messages inserted into the database will be
	MaxOutputMessageSizeBytes int
	// Number of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time after which events will be deleted from the db
	EventRetentionPolicy EventRetentionPolicy
	// List of Regexes which will identify fatal errors when inserting into redis
	FatalInsertionErrors []string
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}

type MetricsConfig struct {
	Redis                   RedisMemoryMetricsConfig
	EventSizeMetricsEnabled bool
}

type RedisMemoryMetricsConfig struct {
	Enabled            bool
	CollectionInterval time.Duration
	// InitialCollectionDelayMax controls startup jitter before first collection.
	// If zero, a default of 1 minute is used.
	InitialCollectionDelayMax time.Duration
	TopN                      int
	ScanBatchSize             int64
	PipelineBatchSize         int
	InterBatchDelay           time.Duration
	MemoryUsageSamples        int
	Leader                    leaderelection.Config
	// CollectionTimeout is the maximum duration of a single scan attempt.
	// If zero, a default of 5 minutes is used. Set to a negative value to disable.
	CollectionTimeout time.Duration
	// RetryInitialBackoff is the initial backoff between scan attempts after a retryable error.
	// If zero, a default of 500ms is used. It is capped at RetryMaxBackoff.
	// Validation rejects configs where the initial backoff exceeds RetryMaxBackoff.
	RetryInitialBackoff time.Duration
	// RetryMaxBackoff is the maximum backoff between scan attempts.
	// If zero, a default of 30s is used.
	RetryMaxBackoff time.Duration
	// MaxRetries is the maximum number of retries per collection cycle after the first attempt.
	// If zero, a default of 10 is used. Set to a negative value to disable retries.
	MaxRetries int
}

// TODO: unpack this into just EventExpirtation
type EventRetentionPolicy struct {
	RetentionDuration time.Duration
}
