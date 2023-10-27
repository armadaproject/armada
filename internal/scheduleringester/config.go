package scheduleringester

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics configuration
	Metrics configuration.MetricsConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Map of allowed priority classes by name
	PriorityClasses map[string]types.PriorityClass
	// Pulsar subscription name
	SubscriptionName string
	// Number of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Time for which the pulsar consumer will wait for a new message before retrying
	PulsarReceiveTimeout time.Duration
	// Time for which the pulsar consumer will back off after receiving an error on trying to receive a message
	PulsarBackoffTime time.Duration
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	PprofPort *uint16
}
