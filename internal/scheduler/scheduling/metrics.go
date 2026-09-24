package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for the JobDb queued-demand aggregate shadow comparison.
//
// The aggregate-derived queued demand is always computed alongside the legacy
// per-job calculation and compared against it; the legacy calculation remains
// authoritative. The comparisons counter gives the denominator, while mismatches
// must remain at zero before the aggregate could ever be allowed to drive
// scheduling. The duration histogram quantifies the performance improvement of
// the aggregate lookup over the legacy scan.
var (
	jobAggregateCanaryComparisons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_comparisons_total",
			Help: "Number of times the JobDb queued-demand aggregate was compared against the legacy per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_mismatches_total",
			Help: "Number of times the JobDb queued-demand aggregate disagreed with the legacy per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatchComponents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_mismatch_components_total",
			Help: "Number of mismatching components in JobDb queued-demand aggregate canary comparisons, by component.",
		},
		[]string{"pool", "component"},
	)
	jobAggregateCalculationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "armada_scheduler_job_aggregate_calculation_duration_seconds",
			Help: "Time spent deriving queued demand via the legacy scan vs the aggregate lookup.",
			Buckets: []float64{
				0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
			},
		},
		[]string{"pool", "source"},
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

func observeJobAggregateCalculationDuration(pool, source string, seconds float64) {
	jobAggregateCalculationDuration.WithLabelValues(pool, source).Observe(seconds)
}
