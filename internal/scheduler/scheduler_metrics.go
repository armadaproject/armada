package scheduler

import (
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const NAMESPACE = "armada"
const SUBSYSTEM = "scheduler"

type SchedulerMetrics struct {
	// Cycle time when scheduling, as leader.
	ScheduleCycleTime prometheus.Histogram
	// Cycle time when reconciling, as leader or follower.
	ReconcileCycleTime prometheus.Histogram
	// Number of jobs scheduled per queue
	ScheduledJobs prometheus.HistogramVec
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
			Name:      "reconcile_cycle_times",
			Help:      "Cycle time when outside of a scheduling round.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		},
	)

	scheduledJobs := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "scheduled_jobs",
			Help:      "Number of jobs scheduled each round.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5}, // TODO: parametrise in config
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	prometheus.MustRegister(scheduleCycleTime)
	prometheus.MustRegister(reconcileCycleTime)
	prometheus.MustRegister(scheduledJobs)

	return SchedulerMetrics{
		ScheduleCycleTime:  scheduleCycleTime,
		ReconcileCycleTime: reconcileCycleTime,
		ScheduledJobs:      *scheduledJobs,
	}

}

func (metrics *SchedulerMetrics) ReportScheduleCycleTime(cycleTime float64) {
	metrics.ScheduleCycleTime.Observe(cycleTime)
}

func (metrics *SchedulerMetrics) ReportReconcileCycleTime(cycleTime float64) {
	metrics.ReconcileCycleTime.Observe(cycleTime)
}

func (metrics *SchedulerMetrics) ReportScheduledJobs(scheduledJobs []interfaces.LegacySchedulerJob) {
	jobAggregates := aggregateJobs(scheduledJobs)

	for key, count := range jobAggregates {
		queue := key.Queue
		priorityClassName := key.PriorityClass

		observer, err := metrics.ScheduledJobs.GetMetricWithLabelValues(queue, priorityClassName)

		if err != nil {
			// A metric failure isn't reason to kill the programme.
			log.Error(err)
		}

		observer.Observe(float64(count))
	}
}

type collectionKey struct {
	Queue         string
	PriorityClass string
}

func aggregateJobs[S ~[]E, E interfaces.LegacySchedulerJob](scheduledJobs S) map[collectionKey]int {
	groups := make(map[collectionKey]int)

	for _, job := range scheduledJobs {
		key := collectionKey{Queue: job.GetQueue(), PriorityClass: job.GetPriorityClassName()}
		groups[key] += 1
	}

	return groups
}
