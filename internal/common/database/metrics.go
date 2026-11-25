package database

import (
	"github.com/IBM/pgxpoolprometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics registers a collector that exports the pool's connection statistics (the pgxpool_*
// metrics: acquired, idle and total connections, acquire count and duration, and so on) with registerer.
// A process with more than one pool must pass labels that tell them apart, otherwise registration fails.
func RegisterPoolMetrics(registerer prometheus.Registerer, pool *pgxpool.Pool, labels map[string]string) error {
	if err := registerer.Register(pgxpoolprometheus.NewCollector(pool, labels)); err != nil {
		return errors.WithMessage(err, "error registering postgres pool metrics")
	}
	return nil
}
