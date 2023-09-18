package healthmonitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

const (
	// Health check failure reason indicating the monitored component is unavailable (e.g., due to time-out).
	UnavailableReason string = "unavailable"
	// Health check failure reason indicating too many replicas have timed out.
	InsufficientReplicasAvailableReason string = "insufficientReplicasAvailable"
	// Health check failure reason indicating a component is manually disabled.
	ManuallyDisabledReason string = "manuallyDisabled"
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
	Run(*armadacontext.Context, *logrus.Entry) error
}
