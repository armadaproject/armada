package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
)

type LookoutUIConfig struct {
	CustomTitle string

	// We have a separate flag here (instead of making the Oidc field optional)
	// so that clients can override the server's preference.
	OidcEnabled bool
	Oidc        struct {
		Authority   string
		ClientId    string
		RedirectUrl string
		Scope       string
	}

	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsEnabled        bool
	BinocularsBaseUrlPattern string

	OverviewAutoRefreshMs int
	JobSetsAutoRefreshMs  int
	JobsAutoRefreshMs     int

	LookoutV2ApiBaseUrl string
}

type PrunerConfig struct {
	DaysToKeep int
	BatchSize  int
}

type LookoutConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig

	UIConfig LookoutUIConfig

	Postgres     configuration.PostgresConfig
	PrunerConfig PrunerConfig
}

type LookoutIngesterDebugConfig struct {
	// Disables DB update conflation. If conflation is disabled then update
	// instructions	will not conflated/coalesced. Meaning update instructions
	// that would immediately negate another update on the same table row will
	// be retained.DB updates will also be force to be scalar.
	DisableConflateDBUpdates bool
}

type LookoutIngesterConfiguration struct {
	// Database configuration
	Postgres configuration.PostgresConfig
	// Metrics configuration
	Metrics configuration.MetricsConfig
	// General Pulsar configuration
	Pulsar configuration.PulsarConfig
	// Debug configuration. Not for production use.
	Debug LookoutIngesterDebugConfig
	// Pulsar subscription name
	SubscriptionName string
	// Size in bytes above which job specs will be compressed when inserting in the database
	MinJobSpecCompressionSize int
	// Number of messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// User annotations have a common prefix to avoid clashes with other annotations.  This prefix will be stripped from
	// The annotation before storing in the db
	UserAnnotationPrefix string
}
