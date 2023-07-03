package scheduler

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

const testInstanceName = "instance-1"

var isNotLeaderMetric = prometheus.MustNewConstMetric(leaderStatusDesc, prometheus.GaugeValue, float64(0), testInstanceName)
var isLeaderMetric = prometheus.MustNewConstMetric(leaderStatusDesc, prometheus.GaugeValue, float64(1), testInstanceName)

func TestLeaderStatusMetrics_DefaultsToNotLeader(t *testing.T) {
	collector := NewLeaderStatusMetricsCollector(testInstanceName)

	actual := getCurrentMetrics(collector)
	assert.Len(t, actual, 1)
	assert.Equal(t, actual[0], isNotLeaderMetric)
}

func TestLeaderStatusMetrics_HandlesLeaderChanges(t *testing.T) {
	collector := NewLeaderStatusMetricsCollector(testInstanceName)

	actual := getCurrentMetrics(collector)
	assert.Len(t, actual, 1)
	assert.Equal(t, actual[0], isNotLeaderMetric)

	// start leading
	collector.onStartedLeading(context.Background())
	actual = getCurrentMetrics(collector)
	assert.Len(t, actual, 1)
	assert.Equal(t, actual[0], isLeaderMetric)

	// stop leading
	collector.onStoppedLeading()
	actual = getCurrentMetrics(collector)
	assert.Len(t, actual, 1)
	assert.Equal(t, actual[0], isNotLeaderMetric)
}

func getCurrentMetrics(collector *LeaderStatusMetricsCollector) []prometheus.Metric {
	metricChan := make(chan prometheus.Metric, 1000)
	collector.Collect(metricChan)
	close(metricChan)

	actual := make([]prometheus.Metric, 0)
	for m := range metricChan {
		actual = append(actual, m)
	}
	return actual
}
