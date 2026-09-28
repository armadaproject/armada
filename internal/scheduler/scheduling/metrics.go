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
// scheduling.
//
// Durations are recorded as two separate histograms rather than one, because the
// two measurements cover different scopes and must not be read as a like-for-like
// speedup: the legacy histogram times the whole scheduling-info build, while the
// lookup histogram times only the isolated aggregate queued-demand lookup. The
// isolated scan-vs-aggregate comparison lives in the aggregate benchmarks.
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
	jobAggregateLegacySchedulingInfoDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "armada_scheduler_job_aggregate_legacy_scheduling_info_duration_seconds",
			Help: "Time spent per scheduling round building the full legacy job scheduling info. Recorded alongside the aggregate lookup for context only; it is not a like-for-like comparison with the isolated aggregate queued-demand lookup.",
			Buckets: []float64{
				0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
			},
		},
		[]string{"pool"},
	)
	jobAggregateLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "armada_scheduler_job_aggregate_lookup_duration_seconds",
			Help: "Time spent per scheduling round deriving queued demand from the incrementally maintained JobDb aggregate. This is the isolated counterpart of the legacy queued-demand scan that the aggregate benchmarks compare against.",
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

func observeJobAggregateLegacySchedulingInfoDuration(pool string, seconds float64) {
	jobAggregateLegacySchedulingInfoDuration.WithLabelValues(pool).Observe(seconds)
}

func observeJobAggregateLookupDuration(pool string, seconds float64) {
	jobAggregateLookupDuration.WithLabelValues(pool).Observe(seconds)
}
