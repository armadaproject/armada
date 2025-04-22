package configuration

import (
	"time"

	"github.com/redis/go-redis/v9"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
)

type EventIngesterConfiguration struct {
	// Database configuration
	Redis redis.UniversalOptions
	// Database configuration - write to a second redis with this option
	// Nearly always not set, but can be useful if migrating the armada control plane to another kubernetes cluster
	RedisReplica redis.UniversalOptions
	// Metrics configuration
	MetricsPort uint16
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

// TODO: unpack this into just EventExpirtation
type EventRetentionPolicy struct {
	RetentionDuration time.Duration
}
