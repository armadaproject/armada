package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type LookoutV2Config struct {
	ApiPort            int
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

	ArmadaApiBaseUrl         string
	UserAnnotationPrefix     string
	BinocularsEnabled        bool
	BinocularsBaseUrlPattern string

	OverviewAutoRefreshMs int
	JobSetsAutoRefreshMs  int
	JobsAutoRefreshMs     int
}
