package scheduler

import (
	"time"

	"github.com/armadaproject/armada/internal/common/util"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
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
	// Duplicate of number of jobs scheduled per queue metric.
	scheduledJobsPerQueueDuplicate prometheus.CounterVec
	// Duplicate of number of jobs preempted per queue metric.
	preemptedJobsPerQueueDuplicate prometheus.CounterVec
	// Number of jobs failed per queue.
	failedJobsPerQueue prometheus.CounterVec
	// Number of jobs succeeded per queue.
	succeededJobsPerQueue prometheus.CounterVec
	// Number of jobs considered per queue/pool.
	consideredJobs prometheus.CounterVec
	// Fair share of each queue.
	fairSharePerQueue prometheus.GaugeVec
	// Actual share of each queue.
	actualSharePerQueue prometheus.GaugeVec
	// Number of jobs scheduled per node.
	scheduledJobsPerNode prometheus.CounterVec
	// Number of jobs preempted per node.
	preemptedJobsPerNode prometheus.CounterVec
	// Number of jobs failed per node.
	failedJobsPerNode prometheus.CounterVec
	// Number of jobs succeeded per node.
	succeededJobsPerNode prometheus.CounterVec
	// Determines whether to expose specified metrics
	metricsToExpose map[prometheus.Collector]bool
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

	scheduledJobsPerQueueDuplicate := prometheus.NewCounterVec(
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

	preemptedJobsPerQueueDuplicate := prometheus.NewCounterVec(
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

	scheduledJobsPerQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "scheduled_jobs_per_queue",
			Help:      "Number of jobs scheduled each round.",
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	preemptedJobsPerQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "preempted_jobs_per_queue",
			Help:      "Number of jobs preempted each round.",
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	failedJobsPerQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "failed_jobs_per_queue",
			Help:      "Number of jobs failed per queue in each round.",
		},
		[]string{
			"queue",
			"priority_class",
		},
	)

	succeededJobsPerQueue := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "succeeded_jobs_per_queue",
			Help:      "Number of jobs succeeded per queue in each round.",
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
	scheduledJobsPerNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "scheduled_jobs_per_node",
			Help:      "Number of jobs scheduled per node in each round.",
		},
		[]string{
			"node",
			"priority_class",
		},
	)

	preemptedJobsPerNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "preempted_jobs_per_node",
			Help:      "Number of jobs preempted per node in each round.",
		},
		[]string{
			"node",
			"priority_class",
		},
	)

	failedJobsPerNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "failed_jobs_per_node",
			Help:      "Number of jobs failed per node in each round.",
		},
		[]string{
			"node",
			"priority_class",
		},
	)

	succeededJobsPerNode := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "succeeded_jobs_per_node",
			Help:      "Number of jobs succeeded per node in each round.",
		},
		[]string{
			"node",
			"priority_class",
		},
	)

	// Determines which metrics to gate
	metricsGatingMap := map[prometheus.Collector]bool{
		scheduleCycleTime:              false,
		reconcileCycleTime:             false,
		scheduledJobsPerQueue:          true,
		preemptedJobsPerQueue:          true,
		scheduledJobsPerQueueDuplicate: false,
		preemptedJobsPerQueueDuplicate: false,
		failedJobsPerQueue:             true,
		succeededJobsPerQueue:          true,
		consideredJobs:                 false,
		fairSharePerQueue:              false,
		actualSharePerQueue:            false,
		scheduledJobsPerNode:           true,
		preemptedJobsPerNode:           true,
		failedJobsPerNode:              true,
		succeededJobsPerNode:           true,
	}

	// Determines which metrics to expose
	metricsToExpose := make(map[prometheus.Collector]bool, len(metricsGatingMap))

	for metric, gate := range metricsGatingMap {
		if metricsToExpose[metric] = !gate || config.EnablePerQueueNodeMetrics; metricsToExpose[metric] {
			prometheus.MustRegister(metric)
		}
	}

	return &SchedulerMetrics{
		scheduleCycleTime:              scheduleCycleTime,
		reconcileCycleTime:             reconcileCycleTime,
		scheduledJobsPerQueue:          *scheduledJobsPerQueue,
		preemptedJobsPerQueue:          *preemptedJobsPerQueue,
		scheduledJobsPerQueueDuplicate: *scheduledJobsPerQueueDuplicate,
		preemptedJobsPerQueueDuplicate: *preemptedJobsPerQueueDuplicate,
		succeededJobsPerQueue:          *succeededJobsPerQueue,
		failedJobsPerQueue:             *failedJobsPerQueue,
		consideredJobs:                 *consideredJobs,
		fairSharePerQueue:              *fairSharePerQueue,
		actualSharePerQueue:            *actualSharePerQueue,
		scheduledJobsPerNode:           *scheduledJobsPerNode,
		preemptedJobsPerNode:           *preemptedJobsPerNode,
		succeededJobsPerNode:           *succeededJobsPerNode,
		failedJobsPerNode:              *failedJobsPerNode,
		metricsToExpose:                metricsToExpose,
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

func (metrics *SchedulerMetrics) ReportSchedulerResult(ctx *armadacontext.Context, result SchedulerResult) {
	if len(result.ScheduledJobs) == 0 && len(result.PreemptedJobs) == 0 && len(result.FailedJobs) == 0 {
		return
	}
	nodeFromJob := func(j *jobdb.Job) string {
		if j.HasRuns() {
			return j.LatestRun().NodeName()
		} else {
			return "node_unknown"
		}
	}
	toJobDBJob := func(j interfaces.LegacySchedulerJob) (job *jobdb.Job) {
		defer func() {
			if recover() != nil {
				job = nil
			}
		}()

		return j.(*jobdb.Job)
	}
	toLegacySchedulerJob := func(j *jobdb.Job) interfaces.LegacySchedulerJob { return j }

	scheduledJobs := util.Filter(util.Map(result.ScheduledJobs, toJobDBJob), func(j *jobdb.Job) bool { return j != nil })
	preemptedJobs := util.Filter(util.Map(result.PreemptedJobs, toJobDBJob), func(j *jobdb.Job) bool { return j != nil })

	scheduledJobNodes := util.Map(scheduledJobs, nodeFromJob)
	preemptedJobNodes := util.Map(preemptedJobs, nodeFromJob)

	// Report the total scheduled jobs (possibly we can get these out of contexts?)
	metrics.ReportPerQueueAggregate(ctx, result.ScheduledJobs, metrics.scheduledJobsPerQueue)
	metrics.ReportPerQueueAggregate(ctx, result.PreemptedJobs, metrics.preemptedJobsPerQueue)
	metrics.ReportPerNodeAggregate(ctx, util.Map(scheduledJobs, toLegacySchedulerJob), scheduledJobNodes, metrics.scheduledJobsPerNode)
	metrics.ReportPerNodeAggregate(ctx, util.Map(preemptedJobs, toLegacySchedulerJob), preemptedJobNodes, metrics.preemptedJobsPerNode)

	// TODO: When more metrics are added, consider consolidating into a single loop over the data.
	// Report the number of considered jobs.
	metrics.reportNumberOfJobsConsidered(ctx, result.SchedulingContexts)
	metrics.reportQueueShares(ctx, result.SchedulingContexts)
}

func (metrics *SchedulerMetrics) ReportPerQueueAggregate(ctx *armadacontext.Context, relevantJobs []interfaces.LegacySchedulerJob, counterVec prometheus.CounterVec) {
	if metrics.metricsToExpose[counterVec] {
		jobAggregates := aggregateJobsByAggregator(relevantJobs, util.Map(relevantJobs, func(x interfaces.LegacySchedulerJob) string { return x.GetQueue() }))
		observeJobAggregates(ctx, counterVec, jobAggregates)
	}
}

func (metrics *SchedulerMetrics) ReportPerNodeAggregate(ctx *armadacontext.Context, relevantJobs []interfaces.LegacySchedulerJob, nodes []string, counterVec prometheus.CounterVec) {
	if metrics.metricsToExpose[counterVec] {
		jobAggregates := aggregateJobsByAggregator(relevantJobs, nodes)
		observeJobAggregates(ctx, counterVec, jobAggregates)
	}
}

type aggregatorCollectionKey struct {
	aggregator    string
	priorityClass string
}

// aggregateJobsByQueue takes a list of jobs and associated keys, and counts how many there are of each key, priorityClass pair.
func aggregateJobsByAggregator[S ~[]E, E interfaces.LegacySchedulerJob](scheduledJobs S, aggregators []string) map[aggregatorCollectionKey]int {
	groups := make(map[aggregatorCollectionKey]int)

	for ix, job := range scheduledJobs {
		key := aggregatorCollectionKey{aggregator: aggregators[ix], priorityClass: job.GetPriorityClassName()}
		groups[key] += 1
	}

	return groups
}

// observeJobAggregates reports a set of job aggregates to a given CounterVec by aggregator and priorityClass.
func observeJobAggregates(ctx *armadacontext.Context, metric prometheus.CounterVec, jobAggregates map[aggregatorCollectionKey]int) {
	for collectionKey, count := range jobAggregates {
		aggregator := collectionKey.aggregator
		priorityClassName := collectionKey.priorityClass

		observer, err := metric.GetMetricWithLabelValues(aggregator, priorityClassName)

		if err != nil {
			// A metric failure isn't reason to kill the programme.
			ctx.Errorf("error retrieving considered jobs observer for aggregator %s, priorityClass %s", aggregator, priorityClassName)
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

func (metrics *SchedulerMetrics) reportQueueShares(ctx *armadacontext.Context, schedulingContexts []*schedulercontext.SchedulingContext) {
	for _, schedContext := range schedulingContexts {
		totalCost := schedContext.TotalCost()
		totalWeight := schedContext.WeightSum
		pool := schedContext.Pool

		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			fairShare := queueContext.Weight / totalWeight

			observer, err := metrics.fairSharePerQueue.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				ctx.Errorf("error retrieving considered jobs observer for queue %s, pool %s", queue, pool)
			} else {
				observer.Set(fairShare)
			}

			actualShare := schedContext.FairnessCostProvider.CostFromQueue(queueContext) / totalCost

			observer, err = metrics.actualSharePerQueue.GetMetricWithLabelValues(queue, pool)
			if err != nil {
				ctx.Errorf("error reteriving considered jobs observer for queue %s, pool %s", queue, pool)
			} else {
				observer.Set(actualShare)
			}
		}
	}
}
