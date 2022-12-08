package configuration

import "github.com/G-Research/armada/internal/armada/configuration"

type LookoutV2Configuration struct {
	Port     int
	Postgres configuration.PostgresConfig
}
