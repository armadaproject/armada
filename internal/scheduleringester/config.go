package scheduleringester

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics Port
	MetricsPort uint16
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}
