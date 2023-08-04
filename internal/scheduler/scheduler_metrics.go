package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
)

const NAMESPACE = "armada"
const SUBSYSTEM = "scheduler"

type SchedulerMetrics struct {
	// Cycle time when scheduling, as leader.
	ScheduleCycleTime prometheus.Histogram
	// Cycle time when reconciling, as leader or follower.
	ReconcileCycleTime prometheus.Histogram
}

func NewSchedulerMetrics() SchedulerMetrics {

	scheduleCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "schedule_cycle_times",
			Help:      "Cycle time when in a scheduling round.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		},
	)

	reconcileCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "schedule_cycle_times",
			Help:      "Cycle time when in a scheduling round.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		},
	)

	prometheus.MustRegister(scheduleCycleTime)
	prometheus.MustRegister(reconcileCycleTime)

	return SchedulerMetrics{
		ScheduleCycleTime:  scheduleCycleTime,
		ReconcileCycleTime: reconcileCycleTime,
	}

}

func (metrics *SchedulerMetrics) ReportCycleTime(cycleTime float64) {

}
