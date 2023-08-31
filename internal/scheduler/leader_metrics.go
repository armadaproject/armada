package scheduler

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/internal/common/metrics"
)

var leaderStatusDesc = prometheus.NewDesc(
	metrics.MetricPrefix+"scheduler_leader_status",
	"Gauge of if the reporting system is leader, 0 indicates hot replica, 1 indicates leader.",
	[]string{"name"}, nil,
)

type LeaderStatusMetricsCollector struct {
	currentInstanceName string
	isCurrentlyLeader   bool
	lock                sync.Mutex
}

func NewLeaderStatusMetricsCollector(currentInstanceName string) *LeaderStatusMetricsCollector {
	return &LeaderStatusMetricsCollector{
		isCurrentlyLeader:   false,
		currentInstanceName: currentInstanceName,
		lock:                sync.Mutex{},
	}
}

func (l *LeaderStatusMetricsCollector) onStartedLeading(*context.ArmadaContext) {
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
	desc <- leaderStatusDesc
}

func (l *LeaderStatusMetricsCollector) Collect(metrics chan<- prometheus.Metric) {
	value := float64(0)
	if l.isLeading() {
		value = 1
	}
	metrics <- prometheus.MustNewConstMetric(leaderStatusDesc, prometheus.GaugeValue, value, l.currentInstanceName)
}
