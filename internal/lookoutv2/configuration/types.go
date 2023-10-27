package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type LookoutV2Config struct {
	ApiPort int
	// If non-nil, net/http/pprof endpoints are exposed on localhost on this port.
	PprofPort *uint16

	CorsAllowedOrigins []string
	Tls                TlsConfig

	Postgres configuration.PostgresConfig

	PrunerConfig PrunerConfig

	UIConfig
}

type TlsConfig struct {
	Enabled  bool
	KeyPath  string
	CertPath string
}

type PrunerConfig struct {
	ExpireAfter time.Duration
	Timeout     time.Duration
	BatchSize   int
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

	JobSetsAutoRefreshMs int
	JobsAutoRefreshMs    int
}
