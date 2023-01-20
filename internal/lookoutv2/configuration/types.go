package configuration

import "github.com/armadaproject/armada/internal/armada/configuration"

type LookoutV2Configuration struct {
	ApiPort            int
	CorsAllowedOrigins []string

	Postgres configuration.PostgresConfig
}
