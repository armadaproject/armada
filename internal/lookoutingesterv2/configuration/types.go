package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
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
	// Maximum number of attempts the ingester will try to store batches of data in the database
	// It will give up after this number is reached
	MaxAttempts int
	// Between each attempt to store data in the database, there is an exponential backoff (starting out as 1s).
	// MaxBackoff caps this backoff to whatever it is specified (in seconds)
	MaxBackoff int
	// If the ingester should process events using the legacy event conversion logic
	// The two schedulers produce slightly different events - so need to be processed differently
	UseLegacyEventConversion bool
}
