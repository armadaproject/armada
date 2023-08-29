package healthmonitor

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// ManualHealthMonitor is a manually controlled health monitor.
type ManualHealthMonitor struct {
	isHealthy bool
	mu        sync.Mutex
}

func (srv *ManualHealthMonitor) SetHealthStatus(isHealthy bool) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	previous := srv.isHealthy
	srv.isHealthy = isHealthy
	return previous
}

func (srv *ManualHealthMonitor) IsHealthy() (bool, string, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return srv.isHealthy, "", nil
}

func (srv *ManualHealthMonitor) Run(ctx context.Context, log *logrus.Entry) error {
	return nil
}

func (srv *ManualHealthMonitor) Describe(c chan<- *prometheus.Desc) {
}

func (srv *ManualHealthMonitor) Collect(c chan<- prometheus.Metric) {
}
