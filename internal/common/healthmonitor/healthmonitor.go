package healthmonitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	// Health check failure reason indicating the monitored component has timed out.
	TimedOutReason string = "timedOut"
	// Health check failure reason indicating too many replicas have timed out.
	InsufficientReplicasAvailableReason string = "insufficientReplicasAvailable"
)

// HealthMonitor represents a health checker.
type HealthMonitor interface {
	prometheus.Collector
	// IsHealthy performs a health check,
	// returning the result, a reason (should be empty if successful), and possibly an error.
	IsHealthy() (ok bool, reason string, err error)
	// Run initialises and starts the health checker.
	// Run may be blocking and should be run within a separate goroutine.
	// Must be called before IsHealthy() or any prometheus.Collector interface methods.
	Run(context.Context, *logrus.Entry) error
}
