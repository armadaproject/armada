package scheduling

import (
	"github.com/armadaproject/armada/internal/common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for the JobDb queued-demand aggregate canary comparison.
//
// While the aggregate is validated, queued demand is computed both by scanning
// jobs and from the aggregate, and the two are compared. The comparison counter
// is the denominator; mismatches must stay at zero before the aggregate can be
// used to drive scheduling.
//
// The two durations are kept in separate histograms because they cover
// different scopes and are not a like-for-like speedup: one times the full
// scheduling-info build, the other only the aggregate lookup. The isolated
// scan-vs-aggregate comparison lives in the aggregate benchmarks.
var (
	jobAggregateCanaryComparisons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_canary_comparisons_total",
			Help: "Number of times the JobDb queued-demand aggregate was compared against the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_canary_mismatches_total",
			Help: "Number of times the JobDb queued-demand aggregate disagreed with the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatchComponents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_canary_mismatch_components_total",
			Help: "Number of mismatching components in JobDb queued-demand aggregate canary comparisons, by component.",
		},
		[]string{"pool", "component"},
	)
	jobAggregateSchedulingInfoDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_scheduling_info_duration_seconds",
			Help: "Time spent per scheduling round building the full job scheduling info. Recorded for context only; it is not a like-for-like comparison with the aggregate lookup.",
			Buckets: []float64{
				0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
			},
		},
		[]string{"pool"},
	)
	jobAggregateLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_lookup_duration_seconds",
			Help: "Time spent per scheduling round deriving queued demand from the JobDb aggregate.",
			Buckets: []float64{
				0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
			},
		},
		[]string{"pool"},
	)
)

func recordJobAggregateCanaryResult(pool string, mismatchedComponents []string) {
	jobAggregateCanaryComparisons.WithLabelValues(pool).Inc()
	if len(mismatchedComponents) == 0 {
		return
	}
	jobAggregateCanaryMismatches.WithLabelValues(pool).Inc()
	for _, component := range mismatchedComponents {
		jobAggregateCanaryMismatchComponents.WithLabelValues(pool, component).Inc()
	}
}

func observeJobAggregateSchedulingInfoDuration(pool string, seconds float64) {
	jobAggregateSchedulingInfoDuration.WithLabelValues(pool).Observe(seconds)
}

func observeJobAggregateLookupDuration(pool string, seconds float64) {
	jobAggregateLookupDuration.WithLabelValues(pool).Observe(seconds)
}
