package scheduler

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
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

	mostRecentSchedulingRoundData schedulingRoundData
}

var scheduledJobsDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "scheduled_jobs"),
	"Number of jobs scheduled each round.",
	[]string{
		"queue",
		"priority_class",
	}, nil,
)

var preemptedJobsDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "preempted_jobs"),
	"Number of jobs preempted each round.",
	[]string{
		"queue",
		"priority_class",
	}, nil,
)

var consideredJobsDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "considered_jobs"),
	"Number of jobs considered in the most recent round per queue and pool.",
	[]string{
		"queue",
		"pool",
	}, nil,
)

var fairSharePerQueueDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "fair_share"),
	"Fair share of each queue and pool.",
	[]string{
		"queue",
		"pool",
	}, nil,
)

var actualSharePerQueueDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "actual_share"),
	"Actual share of each queue and pool.",
	[]string{
		"queue",
		"pool",
	}, nil,
)

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

	prometheus.MustRegister(scheduleCycleTime)
	prometheus.MustRegister(reconcileCycleTime)

	return &SchedulerMetrics{
		scheduleCycleTime:  scheduleCycleTime,
		reconcileCycleTime: reconcileCycleTime,
	}
}

func (m *SchedulerMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	m.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *SchedulerMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	m.reconcileCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *SchedulerMetrics) ReportSchedulerResult(result SchedulerResult) {
	currentSchedulingMetrics := schedulingRoundData{
		queuePoolData:    m.calculateQueuePoolMetrics(result.SchedulingContexts),
		scheduledJobData: aggregateJobContexts(m.mostRecentSchedulingRoundData.scheduledJobData, result.ScheduledJobs),
		preemptedJobData: aggregateJobContexts(m.mostRecentSchedulingRoundData.preemptedJobData, result.PreemptedJobs),
	}

	m.mostRecentSchedulingRoundData = currentSchedulingMetrics
}

func (m *SchedulerMetrics) Describe(desc chan<- *prometheus.Desc) {
	desc <- scheduledJobsDesc
	desc <- preemptedJobsDesc
	desc <- consideredJobsDesc
	desc <- fairSharePerQueueDesc
	desc <- actualSharePerQueueDesc
}

func (m *SchedulerMetrics) Collect(metrics chan<- prometheus.Metric) {
	schedulingRoundData := m.mostRecentSchedulingRoundData

	schedulingRoundMetrics := generateSchedulerMetrics(schedulingRoundData)

	for _, m := range schedulingRoundMetrics {
		metrics <- m
	}
}

func generateSchedulerMetrics(schedulingRoundData schedulingRoundData) []prometheus.Metric {
	result := []prometheus.Metric{}

	for key, value := range schedulingRoundData.queuePoolData {
		result = append(result, prometheus.MustNewConstMetric(consideredJobsDesc, prometheus.GaugeValue, float64(value.numberOfJobsConsidered), key.queue, key.pool))
		result = append(result, prometheus.MustNewConstMetric(fairSharePerQueueDesc, prometheus.GaugeValue, float64(value.fairShare), key.queue, key.pool))
		result = append(result, prometheus.MustNewConstMetric(actualSharePerQueueDesc, prometheus.GaugeValue, float64(value.actualShare), key.queue, key.pool))
	}
	for key, value := range schedulingRoundData.scheduledJobData {
		result = append(result, prometheus.MustNewConstMetric(scheduledJobsDesc, prometheus.CounterValue, float64(value), key.queue, key.priorityClass))
	}
	for key, value := range schedulingRoundData.preemptedJobData {
		result = append(result, prometheus.MustNewConstMetric(preemptedJobsDesc, prometheus.CounterValue, float64(value), key.queue, key.priorityClass))
	}

	return result
}

// aggregateJobContexts takes a list of jobs and counts how many there are of each queue, priorityClass pair.
func aggregateJobContexts(previousSchedulingRoundData map[queuePriorityClassKey]int, jctxs []*schedulercontext.JobSchedulingContext) map[queuePriorityClassKey]int {
	result := make(map[queuePriorityClassKey]int)

	for _, jctx := range jctxs {
		job := jctx.Job
		key := queuePriorityClassKey{queue: job.GetQueue(), priorityClass: job.GetPriorityClassName()}
		result[key] += 1
	}

	for key, value := range previousSchedulingRoundData {
		_, present := result[key]
		if present {
			result[key] += value
		} else {
			result[key] = value
		}
	}

	return result
}

func (metrics *SchedulerMetrics) calculateQueuePoolMetrics(schedulingContexts []*schedulercontext.SchedulingContext) map[queuePoolKey]queuePoolData {
	result := make(map[queuePoolKey]queuePoolData)
	for _, schedContext := range schedulingContexts {
		totalCost := schedContext.TotalCost()
		totalWeight := schedContext.WeightSum
		pool := schedContext.Pool

		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			key := queuePoolKey{queue: queue, pool: pool}
			fairShare := queueContext.Weight / totalWeight
			actualShare := schedContext.FairnessCostProvider.CostFromQueue(queueContext) / totalCost

			result[key] = queuePoolData{
				numberOfJobsConsidered: len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts),
				fairShare:              fairShare,
				actualShare:            actualShare,
			}
		}
	}

	return result
}

type schedulingRoundData struct {
	queuePoolData    map[queuePoolKey]queuePoolData
	scheduledJobData map[queuePriorityClassKey]int
	preemptedJobData map[queuePriorityClassKey]int
}

type queuePriorityClassKey struct {
	queue         string
	priorityClass string
}

type queuePoolKey struct {
	queue string
	pool  string
}

type queuePoolData struct {
	numberOfJobsConsidered int
	actualShare            float64
	fairShare              float64
}
