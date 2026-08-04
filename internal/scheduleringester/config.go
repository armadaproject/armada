package scheduleringester

import (
	"errors"
	"time"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/observability"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/server/configuration"
)

type Configuration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics Port
	MetricsPort uint16
	// Configuration controlling OpenTelemetry observability
	Observability observability.ObservabilityConfig
	// General Pulsar configuration
	Pulsar commonconfig.PulsarConfig
	// Overrides Pulsar.DeadLetterMaxAttempts for this ingester specifically. If unset (0),
	// Pulsar.DeadLetterMaxAttempts is used. Since scheduler ingester writes are the most
	// consequential to dead-letter, this lets scheduler-specific tuning be made without
	// changing behaviour for the event/lookout ingesters, which share the same Pulsar config.
	DeadLetterMaxAttemptsOverride int `validate:"omitempty,gte=2"`
	// Overrides Pulsar.MaxBackoffTime for this ingester specifically. If unset (<= 0),
	// Pulsar.MaxBackoffTime is used. See DeadLetterMaxAttemptsOverride.
	MaxBackoffTimeOverride time.Duration
	// Pulsar subscription name
	SubscriptionName string
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}

func (c *Configuration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("scheduleringester")
	return c, nil
}

func (c Configuration) Validate() error {
	validate := validator.New()
	return errors.Join(validate.Struct(c), c.Pulsar.Validate(), c.effectivePulsarConfig().Validate())
}

// effectivePulsarConfig returns c.Pulsar with DeadLetterMaxAttemptsOverride and
// MaxBackoffTimeOverride applied, for use when constructing the ingestion pipelines.
func (c Configuration) effectivePulsarConfig() commonconfig.PulsarConfig {
	pulsarConfig := c.Pulsar
	if c.DeadLetterMaxAttemptsOverride > 0 {
		pulsarConfig.DeadLetterMaxAttempts = c.DeadLetterMaxAttemptsOverride
	}
	if c.MaxBackoffTimeOverride > 0 {
		pulsarConfig.MaxBackoffTime = c.MaxBackoffTimeOverride
	}
	return pulsarConfig
}
