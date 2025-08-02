package clickhouseingester

import (
	"time"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
)

type ClickHouseConfig map[string]string

// Configuration is the config object for the Clickhouse ingester
type Configuration struct {
	// Port on which prometheus metrics will be served
	MetricsPort uint16
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Pulsar Subscription Configuration
	Pulsar commonconfig.PulsarConfig
	// Clickhouse Configuration
	ClickHouse ClickHouseConfig
	// User annotations have a common prefix to avoid clashes with other annotations.  This prefix will be stripped from
	// The annotation before storing in the db
	UserAnnotationPrefix string
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}
