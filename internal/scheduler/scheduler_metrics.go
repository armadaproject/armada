package scheduler

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
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
	// This is used to allow us to delete old gauge values
	previousQueueShares map[queueShareKey]queueShareValue
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

func (metrics *SchedulerMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	metrics.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (metrics *SchedulerMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	metrics.reconcileCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (metrics *SchedulerMetrics) ReportSchedulerResult(ctx *armadacontext.Context, result SchedulerResult) {
	// Report the total scheduled jobs (possibly we can get these out of contexts?)
	metrics.reportScheduledJobs(ctx, result.ScheduledJobs)
	metrics.reportPreemptedJobs(ctx, result.PreemptedJobs)

	// TODO: When more metrics are added, consider consolidating into a single loop over the data.
	// Report the number of considered jobs.
	metrics.reportNumberOfJobsConsidered(ctx, result.SchedulingContexts)
	metrics.reportQueueShares(ctx, result.SchedulingContexts)
}

func (metrics *SchedulerMetrics) reportScheduledJobs(ctx *armadacontext.Context, scheduledJobs []*schedulercontext.JobSchedulingContext) {
	if len(scheduledJobs) == 0 {
		return
	}
	jobAggregates := aggregateJobContexts(scheduledJobs)
	observeJobAggregates(ctx, metrics.scheduledJobsPerQueue, jobAggregates)
}

func (metrics *SchedulerMetrics) reportPreemptedJobs(ctx *armadacontext.Context, preemptedJobs []*schedulercontext.JobSchedulingContext) {
	if len(preemptedJobs) == 0 {
		return
	}
	jobAggregates := aggregateJobContexts(preemptedJobs)
	observeJobAggregates(ctx, metrics.preemptedJobsPerQueue, jobAggregates)
}

type collectionKey struct {
	queue         string
	priorityClass string
}

// aggregateJobContexts takes a list of jobs and counts how many there are of each queue, priorityClass pair.
func aggregateJobContexts(jctxs []*schedulercontext.JobSchedulingContext) map[collectionKey]int {
	groups := make(map[collectionKey]int)

	for _, jctx := range jctxs {
		job := jctx.Job
		key := collectionKey{queue: job.GetQueue(), priorityClass: job.GetPriorityClassName()}
		groups[key] += 1
	}

	return groups
}

// observeJobAggregates reports a set of job aggregates to a given CounterVec by queue and priorityClass.
func observeJobAggregates(ctx *armadacontext.Context, metric prometheus.CounterVec, jobAggregates map[collectionKey]int) {
	for key, count := range jobAggregates {
		queue := key.queue
		priorityClassName := key.priorityClass

		observer, err := metric.GetMetricWithLabelValues(queue, priorityClassName)

		if err != nil {
			// A metric failure isn't reason to kill the programme.
			ctx.Errorf("error reteriving considered jobs observer for queue %s, priorityClass %s", queue, priorityClassName)
		} else {
			observer.Add(float64(count))
		}
	}
}

func (metrics *SchedulerMetrics) reportNumberOfJobsConsidered(ctx *armadacontext.Context, schedulingContexts []*schedulercontext.SchedulingContext) {
	for _, schedContext := range schedulingContexts {
		pool := schedContext.Pool
		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			count := len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts)

			observer, err := metrics.consideredJobs.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				ctx.Errorf("error reteriving considered jobs observer for queue %s, pool %s", queue, pool)
			} else {
				observer.Add(float64(count))
			}
		}
	}
}

type queueShareKey struct {
	queue string
	pool  string
}

type queueShareValue struct {
	actualShare float64
	fairShare   float64
}

func (metrics *SchedulerMetrics) reportQueueShares(ctx *armadacontext.Context, schedulingContexts []*schedulercontext.SchedulingContext) {
	currentQueueShares := metrics.calculateQueueShares(schedulingContexts)

	for key, _ := range metrics.previousQueueShares {
		if _, present := currentQueueShares[key]; !present {
			metrics.fairSharePerQueue.DeleteLabelValues(key.queue, key.pool)
			metrics.actualSharePerQueue.DeleteLabelValues(key.queue, key.pool)
		}
	}

	for key, value := range currentQueueShares {
		observer, err := metrics.fairSharePerQueue.GetMetricWithLabelValues(key.queue, key.pool)
		if err != nil {
			ctx.Errorf("error retrieving considered jobs observer for queue %s, pool %s", key.queue, key.pool)
		} else {
			observer.Set(value.fairShare)
		}

		observer, err = metrics.actualSharePerQueue.GetMetricWithLabelValues(key.queue, key.pool)
		if err != nil {
			ctx.Errorf("error retrieving considered jobs observer for queue %s, pool %s", key.queue, key.pool)
		} else {
			observer.Set(value.actualShare)
		}
	}

	metrics.previousQueueShares = currentQueueShares
}

func (metrics *SchedulerMetrics) calculateQueueShares(schedulingContexts []*schedulercontext.SchedulingContext) map[queueShareKey]queueShareValue {
	result := make(map[queueShareKey]queueShareValue)
	for _, schedContext := range schedulingContexts {
		totalCost := schedContext.TotalCost()
		totalWeight := schedContext.WeightSum
		pool := schedContext.Pool

		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			key := queueShareKey{queue: queue, pool: pool}
			fairShare := queueContext.Weight / totalWeight
			actualShare := schedContext.FairnessCostProvider.CostFromQueue(queueContext) / totalCost

			result[key] = queueShareValue{fairShare: fairShare, actualShare: actualShare}
		}
	}

	return result
}
