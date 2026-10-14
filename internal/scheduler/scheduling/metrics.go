package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/common/metrics"
)

// Metrics for the JobDb queued-demand aggregate comparison.
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
	jobAggregateComparisons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_comparisons_total",
			Help: "Number of times the JobDb queued-demand aggregate was compared against the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateMismatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_mismatches_total",
			Help: "Number of times the JobDb queued-demand aggregate disagreed with the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateMismatchComponents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_mismatch_components_total",
			Help: "Number of mismatching components in JobDb queued-demand aggregate comparisons, by component.",
		},
		[]string{"pool", "component"},
	)
	jobAggregateSchedulingInfoDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_scheduling_info_duration_seconds",
			Help: "Time spent per scheduling round building the full job scheduling info. Recorded for context only; it is not a like-for-like comparison with the aggregate lookup.",
			Buckets: []float64{
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"pool"},
	)
	jobAggregateLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_lookup_duration_seconds",
			Help: "Time spent per scheduling round deriving queued demand from the JobDb aggregate.",
			Buckets: []float64{
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"pool"},
	)
)

// Metrics for the JobDb running-jobs aggregate comparison. These mirror the
// queued metrics above so that queued and running rollouts can be observed
// independently.
var (
	jobAggregateRunningComparisons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_running_comparisons_total",
			Help: "Number of times the JobDb running-jobs aggregate was compared against the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateRunningMismatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_running_mismatches_total",
			Help: "Number of times the JobDb running-jobs aggregate disagreed with the per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateRunningMismatchComponents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_running_mismatch_components_total",
			Help: "Number of mismatching components in JobDb running-jobs aggregate comparisons, by component.",
		},
		[]string{"pool", "component"},
	)
	jobAggregateRunningSchedulingInfoDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_running_scheduling_info_duration_seconds",
			Help: "Time spent per scheduling round building the running-jobs scheduling info by scanning. Recorded for context only; it is not a like-for-like comparison with the aggregate lookup.",
			Buckets: []float64{
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"pool"},
	)
	jobAggregateRunningLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: metrics.MetricPrefix + "scheduler_job_aggregate_running_lookup_duration_seconds",
			Help: "Time spent per scheduling round deriving running-jobs scheduling info from the JobDb aggregate.",
			Buckets: []float64{
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"pool"},
	)
)

func observeJobAggregateSchedulingInfoDuration(pool string, seconds float64) {
	jobAggregateSchedulingInfoDuration.WithLabelValues(pool).Observe(seconds)
}

func observeJobAggregateLookupDuration(pool string, seconds float64) {
	jobAggregateLookupDuration.WithLabelValues(pool).Observe(seconds)
}

func observeJobAggregateRunningSchedulingInfoDuration(pool string, seconds float64) {
	jobAggregateRunningSchedulingInfoDuration.WithLabelValues(pool).Observe(seconds)
}

func observeJobAggregateRunningLookupDuration(pool string, seconds float64) {
	jobAggregateRunningLookupDuration.WithLabelValues(pool).Observe(seconds)
}

// recordJobAggregateResult records a queued aggregate comparison and any
// mismatching components.
func recordJobAggregateResult(pool string, mismatchedComponents []string) {
	jobAggregateComparisons.WithLabelValues(pool).Inc()
	if len(mismatchedComponents) == 0 {
		return
	}
	jobAggregateMismatches.WithLabelValues(pool).Inc()
	for _, component := range mismatchedComponents {
		jobAggregateMismatchComponents.WithLabelValues(pool, component).Inc()
	}
}

// recordJobAggregateRunningResult records a running aggregate comparison and any
// mismatching components.
func recordJobAggregateRunningResult(pool string, mismatchedComponents []string) {
	jobAggregateRunningComparisons.WithLabelValues(pool).Inc()
	if len(mismatchedComponents) == 0 {
		return
	}
	jobAggregateRunningMismatches.WithLabelValues(pool).Inc()
	for _, component := range mismatchedComponents {
		jobAggregateRunningMismatchComponents.WithLabelValues(pool, component).Inc()
	}
}
