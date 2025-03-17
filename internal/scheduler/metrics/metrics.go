package metrics

import (
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
)

// Metrics is the top level scheduler metrics.
type Metrics struct {
	*cycleMetrics
	*jobStateMetrics
	leaderMetricsEnabled bool
}

type resettableMetric interface {
	prometheus.Collector
	Reset()
}

func New(errorRegexes []string, trackedResourceNames []v1.ResourceName, jobCheckpointIntervals []time.Duration, jobStateMetricsResetInterval time.Duration) (*Metrics, error) {
	compiledErrorRegexes := make([]*regexp.Regexp, len(errorRegexes))
	for i, errorRegex := range errorRegexes {
		if r, err := regexp.Compile(errorRegex); err != nil {
			return nil, errors.WithStack(err)
		} else {
			compiledErrorRegexes[i] = r
		}
	}
	return &Metrics{
		cycleMetrics:    newCycleMetrics(),
		jobStateMetrics: newJobStateMetrics(compiledErrorRegexes, trackedResourceNames, jobCheckpointIntervals, jobStateMetricsResetInterval),
	}, nil
}

// DisableLeaderMetrics stops leader metrics from being produced.  This is necessary because we only produce
// some metrics when we are leader in order to avoid double counting
func (m *Metrics) DisableLeaderMetrics() {
	m.jobStateMetrics.disable()
	m.cycleMetrics.disableLeaderMetrics()
	m.leaderMetricsEnabled = false
}

// EnableLeaderMetrics starts the job state and cycle metrics produced when scheduler is the leader
func (m *Metrics) EnableLeaderMetrics() {
	m.jobStateMetrics.enable()
	m.cycleMetrics.enableLeaderMetrics()
	m.leaderMetricsEnabled = true
}

// LeaderMetricsEnabled returns true if leader metrics are enabled
func (m *Metrics) LeaderMetricsEnabled() bool {
	return m.leaderMetricsEnabled
}

// Describe is necessary to implement the prometheus.Collector interface
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.jobStateMetrics.describe(ch)
	m.cycleMetrics.describe(ch)
}

// Collect is necessary to implement the prometheus.Collector interface
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.jobStateMetrics.collect(ch)
	m.cycleMetrics.collect(ch)
}
