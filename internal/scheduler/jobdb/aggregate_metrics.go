package jobdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/metrics"
)

var jobAggregateInvariantViolations = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.MetricPrefix + "scheduler_job_aggregate_invariant_violations_total",
		Help: "Number of JobDb queued-demand aggregate invariant violations, by kind.",
	},
	[]string{"kind"},
)

// recordAggregateInvariantViolation reports an inconsistency between a job's
// contribution and the aggregate, indicating that the aggregate has drifted.
func recordAggregateInvariantViolation(kind string) {
	jobAggregateInvariantViolations.WithLabelValues(kind).Inc()
	log.Errorf("JobDb queued-demand aggregate invariant violation: %s", kind)
}
