package metrics

import (
	"database/sql"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

type LookoutDbMetricsProvider interface {
	GetOpenConnections() int
	GetOpenConnectionsUtilization() float64
}

type LookoutSqlDbMetricsProvider struct {
	db             *sql.DB
	postgresConfig configuration.PostgresConfig
}

func NewLookoutSqlDbMetricsProvider(db *sql.DB, postgresConfig configuration.PostgresConfig) *LookoutSqlDbMetricsProvider {
	return &LookoutSqlDbMetricsProvider{
		db:             db,
		postgresConfig: postgresConfig,
	}
}

func (provider *LookoutSqlDbMetricsProvider) GetOpenConnections() int {
	return provider.db.Stats().OpenConnections
}

func (provider *LookoutSqlDbMetricsProvider) GetOpenConnectionsUtilization() float64 {
	if provider.postgresConfig.MaxOpenConns <= 0 {
		return float64(provider.GetOpenConnections())
	}
	return float64(provider.db.Stats().OpenConnections) / float64(provider.postgresConfig.MaxOpenConns)
}
