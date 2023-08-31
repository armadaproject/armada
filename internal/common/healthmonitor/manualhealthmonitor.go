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
	reason    string
	mu        sync.Mutex
}

func NewManualHealthMonitor() *ManualHealthMonitor {
	return &ManualHealthMonitor{
		reason: ManuallyDisabledReason,
	}
}

func (srv *ManualHealthMonitor) SetHealthStatus(isHealthy bool) bool {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	previous := srv.isHealthy
	srv.isHealthy = isHealthy
	return previous
}

func (srv *ManualHealthMonitor) WithReason(reason string) *ManualHealthMonitor {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.reason = reason
	return srv
}

func (srv *ManualHealthMonitor) IsHealthy() (bool, string, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.isHealthy {
		return true, "", nil
	} else {
		return false, srv.reason, nil
	}
}

func (srv *ManualHealthMonitor) Run(ctx context.Context, log *logrus.Entry) error {
	return nil
}

func (srv *ManualHealthMonitor) Describe(c chan<- *prometheus.Desc) {
}

func (srv *ManualHealthMonitor) Collect(c chan<- prometheus.Metric) {
}
