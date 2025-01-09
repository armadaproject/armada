package configuration

import (
	"time"

	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

type LookoutV2Config struct {
	ApiPort     int
	Profiling   *profilingconfig.ProfilingConfig
	MetricsPort int

	CorsAllowedOrigins []string
	Tls                TlsConfig

	Postgres configuration.PostgresConfig

	SchedulerApiConnection client.ApiConnectionDetails

	PrunerConfig PrunerConfig

	UIConfig
}

type TlsConfig struct {
	Enabled  bool
	KeyPath  string
	CertPath string
}

type PrunerConfig struct {
	ExpireAfter              time.Duration
	DeduplicationExpireAfter time.Duration
	Timeout                  time.Duration
	BatchSize                int
	Postgres                 configuration.PostgresConfig
}

type CommandSpec struct {
	Name     string
	Template string
}

type UIConfig struct {
	CustomTitle string

	// We have a separate flag here (instead of making the Oidc field optional)
	// so that clients can override the server's preference.
	OidcEnabled bool
	Oidc        struct {
		Authority string
		ClientId  string
		Scope     string
	}

	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsBaseUrlPattern string

	JobSetsAutoRefreshMs int `json:",omitempty"`
	JobsAutoRefreshMs    int `json:",omitempty"`
	CommandSpecs         []CommandSpec

	Backend string `json:",omitempty"`
}
