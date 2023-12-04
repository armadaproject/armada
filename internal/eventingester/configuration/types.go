package configuration

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type EventIngesterConfiguration struct {
	// Database configuration
	Redis redis.UniversalOptions
	// Metrics configuration
	Metrics configuration.MetricsConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which event message will be compressed when inserting in the database
	MinMessageCompressionSize int
	// Number of messages that will be batched together before being inserted into the database
	BatchMessages int
	// Size of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time after which events will be deleted from the db
	EventRetentionPolicy EventRetentionPolicy
	// List of Regexes which will identify fatal errors when inserting into redis
	FatalInsertionErrors []string
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	PprofPort *uint16
}

type EventRetentionPolicy struct {
	ExpiryEnabled     bool
	RetentionDuration time.Duration
}
