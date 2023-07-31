package runstate

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/job"
)

func TestJobRunStateStoreMetricsCollector_DefaultsToNoMetrics(t *testing.T) {
	collector := setupJobRunStateMetricCollectorTest([]*job.RunState{})

	actual := getCurrentMetrics(collector)
	assert.Len(t, actual, 0)
}

func TestJobRunStateStoreMetricsCollector_ProducesPhaseCountMetrics(t *testing.T) {
	states := []*job.RunState{
		createJobRunState("queue-1", job.Leased),
		createJobRunState("queue-1", job.Leased),
		createJobRunState("queue-2", job.Leased),

		createJobRunState("queue-1", job.Invalid),
		createJobRunState("queue-1", job.SuccessfulSubmission),
		createJobRunState("queue-1", job.FailedSubmission),
		createJobRunState("queue-1", job.Active),
		createJobRunState("queue-1", job.Missing),
	}
	collector := setupJobRunStateMetricCollectorTest(states)

	expected := []prometheus.Metric{
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 2, "queue-1", job.Leased.String()),
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-2", job.Leased.String()),

		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-1", job.Invalid.String()),
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-1", job.SuccessfulSubmission.String()),
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-1", job.FailedSubmission.String()),
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-1", job.Active.String()),
		prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue, 1, "queue-1", job.Missing.String()),
	}

	actual := getCurrentMetrics(collector)
	assert.Len(t, actual, 7)
	for i := 0; i < len(expected); i++ {
		a1 := actual[i]
		// Metrics are not calculated in a deterministic order, so just check all expected metrics are present
		assert.Contains(t, expected, a1)
	}
}

func createJobRunState(queue string, phase job.RunPhase) *job.RunState {
	return &job.RunState{
		Meta: &job.RunMeta{
			Queue: queue,
			RunId: util.NewULID(),
			JobId: util.NewULID(),
		},
		Phase: phase,
	}
}

func setupJobRunStateMetricCollectorTest(initialJobRuns []*job.RunState) *JobRunStateStoreMetricsCollector {
	stateStore := job.NewJobRunStateStoreWithInitialState(initialJobRuns)
	collector := NewJobRunStateStoreMetricsCollector(stateStore)
	return collector
}

func getCurrentMetrics(collector *JobRunStateStoreMetricsCollector) []prometheus.Metric {
	metricChan := make(chan prometheus.Metric, 1000)
	collector.Collect(metricChan)
	close(metricChan)

	actual := make([]prometheus.Metric, 0)
	for m := range metricChan {
		actual = append(actual, m)
	}
	return actual
}
