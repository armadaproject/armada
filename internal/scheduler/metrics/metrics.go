package metrics

import (
	"regexp"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
)

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

func (m *Metrics) DisableJobStateMetrics() {
	if m != nil {
		m.jobStateMetricsDisabled = true
	}
}

func (m *Metrics) Enable() {
	if m != nil {
		m.jobStateMetricsDisabled = false
	}
}

func (m *Metrics) IsDisabled() bool {
	return m.jobStateMetricsDisabled
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	if !m.jobStateMetricsDisabled {
		m.jobStateMetrics.describe(ch)
	}
	m.cycleMetrics.describe(ch)
}

// Collect and then reset all metrics.
// Resetting ensures we do not build up a large number of counters over time.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	if !m.jobStateMetricsDisabled {
		m.jobStateMetrics.collect(ch)
	}
	m.cycleMetrics.collect(ch)
}
