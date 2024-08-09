package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
)

var (
	poolAndQueueLabels          = []string{poolLabel, queueLabel}
	queueAndPriorityClassLabels = []string{queueLabel, priorityClassLabel}
)

type cycleMetrics struct {
	scheduledJobs           *prometheus.CounterVec
	premptedJobs            *prometheus.CounterVec
	consideredJobs          *prometheus.GaugeVec
	fairShare               *prometheus.GaugeVec
	adjustedFairShare       *prometheus.GaugeVec
	actualShare             *prometheus.GaugeVec
	fairnessError           *prometheus.GaugeVec
	demand                  *prometheus.GaugeVec
	cappedDemand            *prometheus.GaugeVec
	scheduleCycleTime       prometheus.Histogram
	reconciliationCycleTime prometheus.Histogram
}

func newCycleMetrics() *cycleMetrics {
	return &cycleMetrics{
		scheduledJobs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "scheduled_jobs",
				Help: "Number of events scheduled",
			},
			queueAndPriorityClassLabels,
		),

		premptedJobs: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "preempted_jobs",
				Help: "Number of jobs preempted",
			},
			queueAndPriorityClassLabels,
		),

		consideredJobs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "considered_jobs",
				Help: "Number of jobs considered",
			},
			poolAndQueueLabels,
		),

		fairShare: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "fair_share",
				Help: "Fair share of each queue",
			},
			poolAndQueueLabels,
		),

		adjustedFairShare: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "adjusted_fair_share",
				Help: "Adjusted Fair share of each queue",
			},
			poolAndQueueLabels,
		),

		actualShare: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "actual_share",
				Help: "Actual Fair share of each queue",
			},
			poolAndQueueLabels,
		),

		demand: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "demand",
				Help: "Demand of each queue",
			},
			poolAndQueueLabels,
		),

		cappedDemand: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "capped_demand",
				Help: "Capped Demand of each queue and pool.  This differs from demand in that it limits demand by scheduling constraints",
			},
			poolAndQueueLabels,
		),

		fairnessError: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "fairness_error",
				Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
			},
			[]string{poolLabel},
		),

		scheduleCycleTime: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    prefix + "schedule_cycle_times",
				Help:    "Cycle time when in a scheduling round.",
				Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
			},
		),

		reconciliationCycleTime: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    prefix + "reconciliation_cycle_times",
				Help:    "Cycle time when in a scheduling round.",
				Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
			},
		),
	}
}

func (m *cycleMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	m.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	m.reconciliationCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportSchedulerResult(result schedulerresult.SchedulerResult) {
	// Metrics that depend on pool
	for _, schedContext := range result.SchedulingContexts {
		pool := schedContext.Pool
		for queue, queueContext := range schedContext.QueueSchedulingContexts {
			jobsConsidered := float64(len(queueContext.UnsuccessfulJobSchedulingContexts) + len(queueContext.SuccessfulJobSchedulingContexts))
			actualShare := schedContext.FairnessCostProvider.UnweightedCostFromQueue(queueContext)
			demand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.Demand)
			cappedDemand := schedContext.FairnessCostProvider.UnweightedCostFromAllocation(queueContext.CappedDemand)

			m.consideredJobs.WithLabelValues(pool, queue).Set(jobsConsidered)
			m.fairShare.WithLabelValues(pool, queue).Set(queueContext.FairShare)
			m.adjustedFairShare.WithLabelValues(pool, queue).Set(queueContext.AdjustedFairShare)
			m.actualShare.WithLabelValues(pool, queue).Set(actualShare)
			m.demand.WithLabelValues(pool, queue).Set(demand)
			m.cappedDemand.WithLabelValues(pool, queue).Set(cappedDemand)
		}
		m.fairnessError.WithLabelValues(pool).Set(schedContext.FairnessError())
	}

	for _, jobCtx := range result.ScheduledJobs {
		m.scheduledJobs.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}

	for _, jobCtx := range result.PreemptedJobs {
		m.premptedJobs.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}
}

func (m *cycleMetrics) describe(ch chan<- *prometheus.Desc) {
	m.scheduledJobs.Describe(ch)
	m.premptedJobs.Describe(ch)
	m.consideredJobs.Describe(ch)
	m.fairShare.Describe(ch)
	m.adjustedFairShare.Describe(ch)
	m.actualShare.Describe(ch)
	m.fairnessError.Describe(ch)
	m.demand.Describe(ch)
	m.cappedDemand.Describe(ch)
	m.scheduleCycleTime.Describe(ch)
	m.reconciliationCycleTime.Describe(ch)
}

func (m *cycleMetrics) collect(ch chan<- prometheus.Metric) {
	m.scheduledJobs.Collect(ch)
	m.premptedJobs.Collect(ch)
	m.consideredJobs.Collect(ch)
	m.fairShare.Collect(ch)
	m.adjustedFairShare.Collect(ch)
	m.actualShare.Collect(ch)
	m.fairnessError.Collect(ch)
	m.demand.Collect(ch)
	m.cappedDemand.Collect(ch)
	m.scheduleCycleTime.Collect(ch)
	m.reconciliationCycleTime.Collect(ch)
}
