package healthmonitor

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

// MultiHealthMonitor wraps multiple HealthMonitors and itself implements the HealthMonitor interface.
type MultiHealthMonitor struct {
	name string
	// Map from name to the health monitor for each cluster.
	healthMonitorsByName map[string]HealthMonitor

	// Minimum replicas that must be available.
	minimumReplicasAvailable int
	// Prometheus metrics are prefixed with this.
	metricsPrefix string

	// Result of the most recent health check.
	resultOfMostRecentHealthCheck bool
	// Mutex protecting the above values.
	mu sync.Mutex

	healthPrometheusDesc *prometheus.Desc
}

func NewMultiHealthMonitor(name string, healthMonitorsByName map[string]HealthMonitor) *MultiHealthMonitor {
	srv := &MultiHealthMonitor{
		name:                     name,
		minimumReplicasAvailable: len(healthMonitorsByName),
		healthMonitorsByName:     maps.Clone(healthMonitorsByName),
	}
	srv.initialiseMetrics()
	return srv
}

func (srv *MultiHealthMonitor) WithMinimumReplicasAvailable(v int) *MultiHealthMonitor {
	srv.minimumReplicasAvailable = v
	return srv
}

// WithMetricsPrefix adds a prefix to exported Prometheus metrics.
// Must be called before Describe or Collect.
func (srv *MultiHealthMonitor) WithMetricsPrefix(v string) *MultiHealthMonitor {
	srv.metricsPrefix = v
	srv.initialiseMetrics()
	return srv
}

func (srv *MultiHealthMonitor) initialiseMetrics() {
	metricsPrefix := srv.name
	if srv.metricsPrefix != "" {
		metricsPrefix = srv.metricsPrefix + srv.name
	}
	srv.healthPrometheusDesc = prometheus.NewDesc(
		metricsPrefix+"_health",
		fmt.Sprintf("Shows whether %s is healthy.", srv.name),
		nil,
		nil,
	)
}

// IsHealthy returns false if either
// 1. any child is unhealthy for a reason other than timeout or
// 2. at least len(healthMonitorsByName) - minReplicasAvailable + 1 children have timed out.
func (srv *MultiHealthMonitor) IsHealthy() (ok bool, reason string, err error) {
	defer func() {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		srv.resultOfMostRecentHealthCheck = ok
	}()
	replicasAvailable := 0
	for name, healthMonitor := range srv.healthMonitorsByName {
		ok, reason, err = healthMonitor.IsHealthy()
		if err != nil {
			return ok, reason, errors.WithMessagef(err, "failed to check health of %s in %s", name, srv.name)
		}

		// Cluster is unhealthy if any child is unhealthy for a reason other than timeout
		// or if too many have timed out.
		if !ok {
			if reason != UnavailableReason {
				return ok, reason, nil
			}
		} else {
			replicasAvailable++
		}
	}
	if replicasAvailable < srv.minimumReplicasAvailable {
		return false, InsufficientReplicasAvailableReason, nil
	}
	return true, "", nil
}

// Run initialises prometheus metrics and starts any child health checkers.
func (srv *MultiHealthMonitor) Run(ctx context.Context, log *logrus.Entry) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, healthMonitor := range srv.healthMonitorsByName {
		healthMonitor := healthMonitor
		g.Go(func() error { return healthMonitor.Run(ctx, log) })
	}
	return g.Wait()
}

func (srv *MultiHealthMonitor) Describe(c chan<- *prometheus.Desc) {
	c <- srv.healthPrometheusDesc

	for _, healthMonitor := range srv.healthMonitorsByName {
		healthMonitor.Describe(c)
	}
}

func (srv *MultiHealthMonitor) Collect(c chan<- prometheus.Metric) {
	srv.mu.Lock()
	resultOfMostRecentHealthCheck := 0.0
	if srv.resultOfMostRecentHealthCheck {
		resultOfMostRecentHealthCheck = 1.0
	}
	srv.mu.Unlock()
	c <- prometheus.MustNewConstMetric(
		srv.healthPrometheusDesc,
		prometheus.GaugeValue,
		resultOfMostRecentHealthCheck,
		srv.name,
	)

	for _, healthMonitor := range srv.healthMonitorsByName {
		healthMonitor.Collect(c)
	}
}
