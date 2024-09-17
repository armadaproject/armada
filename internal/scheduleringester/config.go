package scheduleringester

import (
	"time"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/server/configuration"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics Port
	MetricsPort uint16
	// General Pulsar configuration
	Pulsar commonconfig.PulsarConfig
	// Pulsar subscription name
	SubscriptionName string
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}

func (c Configuration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}
