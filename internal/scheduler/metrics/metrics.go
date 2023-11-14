package metrics

import (
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	// Indicates whether this instance is the leader.
	isLeader prometheus.Gauge
	// Scheduling cycle time percentiles. Only exported when leader.
	scheduleCycleTime prometheus.Summary
	// State reconciliation cycle time percentiles.
	reconcileCycleTime prometheus.Summary
	// Number of scheduled jobs.
	scheduledJobs prometheus.CounterVec
	// Number of preempted jobs.
	preemptedJobs prometheus.CounterVec
	// Number of failed jobs.
	failedJobs prometheus.CounterVec
	// Number of successful jobs.
	succeededJobs prometheus.CounterVec
	// Number of jobs considered when scheduling.
	consideredJobs prometheus.CounterVec
	// Fair share of each queue.
	fairSharePerQueue prometheus.GaugeVec
	// Actual share of each queue.
	actualSharePerQueue prometheus.GaugeVec
	// Determines whether to expose specified metrics
	disabledMetrics map[prometheus.Collector]bool
}

func (m *Metrics) Observe(result schedulercontext.SchedulingContext) {

}
