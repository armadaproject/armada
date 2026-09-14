package scheduling

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics for the JobDb aggregate shadow comparison.
//
// The aggregate-derived scheduling info is always computed alongside the legacy per-job
// calculation and compared against it; the legacy calculation remains authoritative. The
// comparisons counter gives the denominator, while mismatches must remain at zero before the
// aggregate could ever be allowed to drive scheduling. The component counter localises a
// divergence.
var (
	jobAggregateCanaryComparisons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_comparisons_total",
			Help: "Number of times the JobDb aggregate scheduling info was compared against the legacy per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_mismatches_total",
			Help: "Number of times the JobDb aggregate scheduling info disagreed with the legacy per-job calculation.",
		},
		[]string{"pool"},
	)
	jobAggregateCanaryMismatchComponents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "armada_scheduler_job_aggregate_canary_mismatch_components_total",
			Help: "Number of mismatching components in JobDb aggregate canary comparisons, by component.",
		},
		[]string{"pool", "component"},
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
