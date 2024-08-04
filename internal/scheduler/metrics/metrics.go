package metrics

import (
	"regexp"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
)

// Metrics is the top level scheduler metrics.
type Metrics struct {
	*cycleMetrics
	*jobStateMetrics
	jobStateMetricsDisabled bool
}

func New(errorRegexes []string, trackedResourceNames []v1.ResourceName) (*Metrics, error) {
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
		jobStateMetrics: newJobStateMetrics(compiledErrorRegexes, trackedResourceNames),
	}, nil
}

// DisableJobStateMetrics stops the jobStateMetrics from being produced.  This is necessary because we only produce
// these metrics when we are leaser, so as to avoid double counting
func (m *Metrics) DisableJobStateMetrics() {
	m.jobStateMetricsDisabled = true
}

// EnableJobStateMetrics starts the jobStateMetrics
func (m *Metrics) EnableJobStateMetrics() {
	m.jobStateMetricsDisabled = false
}

// JobStateMetricsDisabled returns true if job state metrics are disabled
func (m *Metrics) JobStateMetricsDisabled() bool {
	return m.jobStateMetricsDisabled
}

// Describe is necessary to implement the prometheus.Collector inteface
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if !m.jobStateMetricsDisabled {
		m.jobStateMetrics.describe(ch)
	}
	m.cycleMetrics.describe(ch)
}

// Collect is necessary to implement the prometheus.Collector inteface
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if !m.jobStateMetricsDisabled {
		m.jobStateMetrics.collect(ch)
	}
	m.cycleMetrics.collect(ch)
}
