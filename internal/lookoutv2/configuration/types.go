package configuration

import "github.com/G-Research/armada/internal/armada/configuration"

type LookoutV2Configuration struct {
	ApiPort            int
	CorsAllowedOrigins []string

	Postgres configuration.PostgresConfig
}
