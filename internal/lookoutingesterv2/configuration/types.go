package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/armada/configuration"
)

type LookoutIngesterV2Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics configuration
	Metrics configuration.MetricsConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which job specs will be compressed when inserting in the database
	MinJobSpecCompressionSize int
	// Number of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time for which the pulsar consumer will wait for a new message before retrying
	PulsarReceiveTimeout time.Duration
	// Time for which the pulsar consumer will back off after receiving an error on trying to receive a message
	PulsarBackoffTime time.Duration
	// User annotations have a common prefix to avoid clashes with other annotations.  This prefix will be stripped from
	// The annotation before storing in the db
	UserAnnotationPrefix string
}
