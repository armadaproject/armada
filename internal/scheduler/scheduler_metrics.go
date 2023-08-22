package scheduler

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	NAMESPACE = "armada"
	SUBSYSTEM = "scheduler"
)

type SchedulerMetrics struct {
	// Cycle time when scheduling, as leader.
	scheduleCycleTime prometheus.Histogram
	// Cycle time when reconciling, as leader or follower.
	reconcileCycleTime prometheus.Histogram
	// Number of jobs scheduled per queue.
	scheduledJobsPerQueue prometheus.CounterVec
	// Number of jobs preempted per queue.
	preemptedJobsPerQueue prometheus.CounterVec
	// Number of jobs considered per queue/pool.
	consideredJobs prometheus.CounterVec
	// Fair share of each queue.
	fairSharePerQueue prometheus.GaugeVec
	// Actual share of each queue.
	actualSharePerQueue prometheus.GaugeVec
}

func NewSchedulerMetrics(config configuration.SchedulerMetricsConfig) *SchedulerMetrics {
	scheduleCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "schedule_cycle_times",
			Help:      "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(
				config.ScheduleCycleTimeHistogramSettings.Start,
				config.ScheduleCycleTimeHistogramSettings.Factor,
				config.ScheduleCycleTimeHistogramSettings.Count),
		},
	)

	reconcileCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "reconcile_cycle_times",
			Help:      "Cycle time when outside of a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(
				config.ReconcileCycleTimeHistogramSettings.Start,
				config.ReconcileCycleTimeHistogramSettings.Factor,
				config.ReconcileCycleTimeHistogramSettings.Count),
		},
	)

	scheduledJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "scheduled_jobs",
			Help:      "Number of jobs scheduled each round.",
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	preemptedJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "preempted_jobs",
			Help:      "Number of jobs preempted each round.",
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	consideredJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "considered_jobs",
			Help:      "Number of jobs considered each round per queue and pool.",
		},
		[]string{
			"queue",
			"pool",
		},
	)

	fairSharePerQueue := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "fair_share",
			Help:      "Fair share of each queue and pool.",
		},
		[]string{
			"queue",
			"pool",
		},
	)

	actualSharePerQueue := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "actual_share",
			Help:      "Actual share of each queue and pool.",
		},
		[]string{
			"queue",
			"pool",
		},
	)

	prometheus.MustRegister(scheduleCycleTime)
	prometheus.MustRegister(reconcileCycleTime)
	prometheus.MustRegister(scheduledJobs)
	prometheus.MustRegister(preemptedJobs)
	prometheus.MustRegister(consideredJobs)
	prometheus.MustRegister(fairSharePerQueue)
	prometheus.MustRegister(actualSharePerQueue)

	return &SchedulerMetrics{
		scheduleCycleTime:     scheduleCycleTime,
		reconcileCycleTime:    reconcileCycleTime,
		scheduledJobsPerQueue: *scheduledJobs,
		preemptedJobsPerQueue: *preemptedJobs,
		consideredJobs:        *consideredJobs,
		fairSharePerQueue:     *fairSharePerQueue,
		actualSharePerQueue:   *actualSharePerQueue,
	}
}

func (metrics *SchedulerMetrics) ResetGaugeMetrics() {
	metrics.fairSharePerQueue.Reset()
	metrics.actualSharePerQueue.Reset()
}

func (metrics *SchedulerMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	metrics.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (metrics *SchedulerMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	metrics.reconcileCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (metrics *SchedulerMetrics) ReportSchedulerResult(result *SchedulerResult) {
	// Report the total scheduled jobs (possibly we can get these out of contexts?)
	metrics.reportScheduledJobs(result.ScheduledJobs)
	metrics.reportPreemptedJobs(result.PreemptedJobs)

	// TODO: When more metrics are added, consider consolidating into a single loop over the data.
	// Report the number of considered jobs.
	metrics.reportNumberOfJobsConsidered(result.SchedulingContexts)
	metrics.reportQueueShares(result.SchedulingContexts)
}

func (metrics *SchedulerMetrics) reportScheduledJobs(scheduledJobs []interfaces.LegacySchedulerJob) {
	jobAggregates := aggregateJobs(scheduledJobs)
	observeJobAggregates(metrics.scheduledJobsPerQueue, jobAggregates)
}

func (metrics *SchedulerMetrics) reportPreemptedJobs(preemptedJobs []interfaces.LegacySchedulerJob) {
	jobAggregates := aggregateJobs(preemptedJobs)
	observeJobAggregates(metrics.preemptedJobsPerQueue, jobAggregates)
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

// observeJobAggregates reports a set of job aggregates to a given CounterVec by queue and priorityClass.
func observeJobAggregates(metric prometheus.CounterVec, jobAggregates map[collectionKey]int) {
	for key, count := range jobAggregates {
		queue := key.queue
		priorityClassName := key.priorityClass

		observer, err := metric.GetMetricWithLabelValues(queue, priorityClassName)

		if err != nil {
			// A metric failure isn't reason to kill the programme.
			log.Error(err)
		} else {
			observer.Add(float64(count))
		}
	}
}

func (metrics *SchedulerMetrics) reportNumberOfJobsConsidered(schedulingContexts []*schedulercontext.SchedulingContext) {
	for _, schedContext := range schedulingContexts {
		pool := schedContext.Pool
		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			count := len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts)

			observer, err := metrics.consideredJobs.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				log.Error(err)
			} else {
				observer.Add(float64(count))
			}
		}
	}
}

func (metrics *SchedulerMetrics) reportQueueShares(schedulingContexts []*schedulercontext.SchedulingContext) {
	for _, schedContext := range schedulingContexts {
		totalCost := schedContext.TotalCost()
		totalWeight := schedContext.WeightSum
		pool := schedContext.Pool

		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			fairShare := queueContext.Weight / totalWeight

			observer, err := metrics.fairSharePerQueue.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				log.Error(err)
			} else {
				observer.Set(fairShare)
			}

			actualShare := schedContext.FairnessCostProvider.CostFromQueue(queueContext) / totalCost

			observer, err = metrics.actualSharePerQueue.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				log.Error(err)
			} else {
				observer.Set(actualShare)
			}
		}
	}
}
