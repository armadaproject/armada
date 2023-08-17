package scheduler

import (
	"sync"

	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const NAMESPACE = "armada"
const SUBSYSTEM = "scheduler"

type SchedulerMetrics struct {
	// Cycle time when scheduling, as leader.
	scheduleCycleTime prometheus.Histogram
	// Cycle time when reconciling, as leader or follower.
	reconcileCycleTime prometheus.Histogram
	// Number of jobs scheduled per queue.
	scheduledJobs prometheus.HistogramVec
	// Number of jobs preempted per queue.
	preemptedJobs prometheus.HistogramVec
}

var schedulerMetricsPointer *SchedulerMetrics
var singletonLock sync.Mutex

func GetSchedulerMetrics() *SchedulerMetrics {
	singletonLock.Lock()
	defer singletonLock.Unlock()

	if schedulerMetricsPointer == nil {
		schedulerMetricsPointer = newSchedulerMetrics()
	}

	return schedulerMetricsPointer
}

func newSchedulerMetrics() *SchedulerMetrics {

	scheduleCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "schedule_cycle_times",
			Help:      "Cycle time when in a scheduling round.",
			Buckets:   prometheus.LinearBuckets(0, 5, 20),
		},
	)

	reconcileCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "reconcile_cycle_times",
			Help:      "Cycle time when outside of a scheduling round.",
			Buckets:   prometheus.LinearBuckets(0, 5, 20),
		},
	)

	scheduledJobs := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "scheduled_jobs",
			Help:      "Number of jobs scheduled each round.",
			Buckets:   prometheus.LinearBuckets(0, 5, 20), // TODO: parametrise in config
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	preemptedJobs := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "preempted_jobs",
			Help:      "Number of jobs preempted each round.",
			Buckets:   prometheus.LinearBuckets(0, 5, 20), // TODO: parametrise in config
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	prometheus.MustRegister(scheduleCycleTime)
	prometheus.MustRegister(reconcileCycleTime)
	prometheus.MustRegister(scheduledJobs)
	prometheus.MustRegister(preemptedJobs)

	return &SchedulerMetrics{
		scheduleCycleTime:  scheduleCycleTime,
		reconcileCycleTime: reconcileCycleTime,
		scheduledJobs:      *scheduledJobs,
		preemptedJobs:      *preemptedJobs,
	}

}

func (metrics *SchedulerMetrics) ReportScheduleCycleTime(cycleTime float64) {
	metrics.scheduleCycleTime.Observe(cycleTime)
}

func (metrics *SchedulerMetrics) ReportReconcileCycleTime(cycleTime float64) {
	metrics.reconcileCycleTime.Observe(cycleTime)
}

func (metrics *SchedulerMetrics) ReportSchedulerResult(result *SchedulerResult) {
	metrics.reportScheduledJobs(result.ScheduledJobs)
	metrics.reportPreemptedJobs(result.PreemptedJobs)
}

func (metrics *SchedulerMetrics) reportScheduledJobs(scheduledJobs []interfaces.LegacySchedulerJob) {
	jobAggregates := aggregateJobs(scheduledJobs)
	observeJobAggregates(metrics.scheduledJobs, jobAggregates)
}

func (metrics *SchedulerMetrics) reportPreemptedJobs(preemptedJobs []interfaces.LegacySchedulerJob) {
	jobAggregates := aggregateJobs(preemptedJobs)
	observeJobAggregates(metrics.preemptedJobs, jobAggregates)
}

type collectionKey struct {
	queue         string
	priorityClass string
}

// aggregateJobs takes a list of jobs and counts how many there are of each queue, priorityClass pair.
func aggregateJobs[S ~[]E, E interfaces.LegacySchedulerJob](scheduledJobs S) map[collectionKey]int {
	groups := make(map[collectionKey]int)

	for _, job := range scheduledJobs {
		key := collectionKey{queue: job.GetQueue(), priorityClass: job.GetPriorityClassName()}
		groups[key] += 1
	}

	return groups
}

// observeJobAggregates reports a set of job aggregates to a given HistogramVec by queue and priorityClass.
func observeJobAggregates(metric prometheus.HistogramVec, jobAggregates map[collectionKey]int) {
	for key, count := range jobAggregates {
		queue := key.queue
		priorityClassName := key.priorityClass

		observer, err := metric.GetMetricWithLabelValues(queue, priorityClassName)

		if err != nil {
			// A metric failure isn't reason to kill the programme.
			log.Error(err)
		} else {
			observer.Observe(float64(count))
		}
	}
}
