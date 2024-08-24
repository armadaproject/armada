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
}

func New(errorRegexes []string, trackedResourceNames []v1.ResourceName, jobStateMetricsResetInterval time.Duration) (*Metrics, error) {
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
		jobStateMetrics: newJobStateMetrics(compiledErrorRegexes, trackedResourceNames, jobStateMetricsResetInterval),
	}, nil
}

// DisableJobStateMetrics stops the jobStateMetrics from being produced.  This is necessary because we only produce
// these metrics when we are leader in order to avoid double counting
func (m *Metrics) DisableJobStateMetrics() {
	m.jobStateMetrics.disable()
}

// EnableJobStateMetrics starts the jobStateMetrics
func (m *Metrics) EnableJobStateMetrics() {
	m.jobStateMetrics.enable()
}

// JobStateMetricsEnabled returns true if job state metrics are enabled
func (m *Metrics) JobStateMetricsEnabled() bool {
	return m.jobStateMetrics.isEnabled()
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
