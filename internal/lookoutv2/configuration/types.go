package configuration

import (
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type LookoutV2Configuration struct {
	ApiPort            int
	CorsAllowedOrigins []string

	Postgres configuration.PostgresConfig

	PrunerConfig PrunerConfig
}

type PrunerConfig struct {
	ExpireAfter time.Duration
	Timeout     time.Duration
	BatchSize   int
}
