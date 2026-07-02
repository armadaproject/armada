package scheduleringester

import (
	"time"

	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/observability"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
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
	// Pulsar subscription name
	SubscriptionName string
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
	// JobMetadataMigrationPhase controls whether submit_message and groups are
	// written to the jobs table, the job_metadata table, or both, during the
	// migration. Required; default ("legacy")
	JobMetadataMigrationPhase schedulerdb.JobMetadataMigrationPhase `validate:"required,oneof=legacy dualWrite cutover"`
}

func (c *Configuration) Mutate() (commonconfig.Config, error) {
	c.Observability.ApplyResourceDefaults("scheduleringester")
	return c, nil
}

func (c Configuration) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}
