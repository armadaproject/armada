package leader

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

type LeaderStatusMetricsCollector struct {
	leaderStatusDesc    *prometheus.Desc
	currentInstanceName string
	isCurrentlyLeader   bool
	lock                sync.Mutex
}

func createLeaderStatusDesc(metricsPrefix string) *prometheus.Desc {
	return prometheus.NewDesc(
		metricsPrefix+"leader_status",
		"Gauge of if the reporting system is leader, 0 indicates hot replica, 1 indicates leader.",
		[]string{"name"}, nil,
	)
}

func NewLeaderStatusMetricsCollector(metricsPrefix string, currentInstanceName string) *LeaderStatusMetricsCollector {
	return &LeaderStatusMetricsCollector{
		isCurrentlyLeader:   false,
		leaderStatusDesc:    createLeaderStatusDesc(metricsPrefix),
		currentInstanceName: currentInstanceName,
		lock:                sync.Mutex{},
	}
}

func (l *LeaderStatusMetricsCollector) onStartedLeading(*armadacontext.Context) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.isCurrentlyLeader = true
}

func (l *LeaderStatusMetricsCollector) onStoppedLeading() {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.isCurrentlyLeader = false
}

func (l *LeaderStatusMetricsCollector) isLeading() bool {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.isCurrentlyLeader
}

func (l *LeaderStatusMetricsCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- l.leaderStatusDesc
}

func (l *LeaderStatusMetricsCollector) Collect(metrics chan<- prometheus.Metric) {
	value := float64(0)
	if l.isLeading() {
		value = 1
	}
	metrics <- prometheus.MustNewConstMetric(l.leaderStatusDesc, prometheus.GaugeValue, value, l.currentInstanceName)
}
