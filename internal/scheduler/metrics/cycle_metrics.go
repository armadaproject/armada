package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

var (
	poolLabels                  = []string{poolLabel}
	poolAndQueueLabels          = []string{poolLabel, queueLabel}
	queueAndPriorityClassLabels = []string{queueLabel, priorityClassLabel}
	poolQueueAndResourceLabels  = []string{poolLabel, queueLabel, resourceLabel}
)

type cycleMetrics struct {
	leaderMetricsEnabled  bool
	cycleMetricAccessLock sync.Mutex

	scheduledJobs             *prometheus.CounterVec
	preemptedJobs             *prometheus.CounterVec
	consideredJobs            *prometheus.GaugeVec
	fairShare                 *prometheus.GaugeVec
	adjustedFairShare         *prometheus.GaugeVec
	actualShare               *prometheus.GaugeVec
	fairnessError             *prometheus.GaugeVec
	demand                    *prometheus.GaugeVec
	cappedDemand              *prometheus.GaugeVec
	queueWeight               *prometheus.GaugeVec
	rawQueueWeight            *prometheus.GaugeVec
	scheduleCycleTime         prometheus.Histogram
	reconciliationCycleTime   prometheus.Histogram
	gangsConsidered           *prometheus.GaugeVec
	gangsScheduled            *prometheus.GaugeVec
	firstGangQueuePosition    *prometheus.GaugeVec
	lastGangQueuePosition     *prometheus.GaugeVec
	perQueueCycleTime         *prometheus.GaugeVec
	loopNumber                *prometheus.GaugeVec
	evictedJobs               *prometheus.GaugeVec
	evictedResources          *prometheus.GaugeVec
	spotPrice                 *prometheus.GaugeVec
	perCycleResettableMetrics []resettableMetric
	allResettableMetrics      []resettableMetric
}

func newCycleMetrics() *cycleMetrics {
	scheduledJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "scheduled_jobs",
			Help: "Number of jobs scheduled",
		},
		queueAndPriorityClassLabels,
	)

	premptedJobs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prefix + "preempted_jobs",
			Help: "Number of jobs preempted",
		},
		queueAndPriorityClassLabels,
	)

	consideredJobs := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "considered_jobs",
			Help: "Number of jobs considered",
		},
		poolAndQueueLabels,
	)

	fairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "fair_share",
			Help: "Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	adjustedFairShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "adjusted_fair_share",
			Help: "Adjusted Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	actualShare := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "actual_share",
			Help: "Actual Fair share of each queue",
		},
		poolAndQueueLabels,
	)

	demand := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "demand",
			Help: "Demand of each queue",
		},
		poolAndQueueLabels,
	)

	cappedDemand := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "capped_demand",
			Help: "Capped Demand of each queue and pool.  This differs from demand in that it limits demand by scheduling constraints",
		},
		poolAndQueueLabels,
	)

	queueWeight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "queue_weight",
			Help: "Weight of the queue after multipliers have been applied",
		},
		poolAndQueueLabels,
	)

	rawQueueWeight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "raw_queue_weight",
			Help: "Weight of the queue before multipliers have been applied",
		},
		poolAndQueueLabels,
	)

	fairnessError := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "fairness_error",
			Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
		},
		[]string{poolLabel},
	)

	scheduleCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    prefix + "schedule_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	reconciliationCycleTime := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    prefix + "reconciliation_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	gangsConsidered := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "gangs_considered",
			Help: "Number of gangs considered in this scheduling cycle",
		},
		poolAndQueueLabels,
	)

	gangsScheduled := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "gangs_scheduled",
			Help: "Number of gangs scheduled in this scheduling cycle",
		},
		poolAndQueueLabels,
	)

	firstGangQueuePosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "first_gang_queue_position",
			Help: "First position in the scheduling loop where a gang was considered",
		},
		poolAndQueueLabels,
	)

	lastGangQueuePosition := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "last_gang_queue_position",
			Help: "Last position in the scheduling loop where a gang was considered",
		},
		poolAndQueueLabels,
	)

	perQueueCycleTime := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "per_queue_schedule_cycle_times",
			Help: "Per queue cycle time when in a scheduling round.",
		},
		poolAndQueueLabels,
	)

	loopNumber := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "loop_number",
			Help: "Number of scheduling loops in this cycle",
		},
		poolLabels,
	)

	evictedJobs := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "evicted_jobs",
			Help: "Number of jobs evicted in this cycle",
		},
		poolAndQueueLabels,
	)

	evictedResources := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "evicted_resources",
			Help: "Resources evicted in this cycle",
		},
		poolQueueAndResourceLabels,
	)

	spotPrice := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prefix + "spot_price",
			Help: "spot price for given pool",
		},
		poolLabels,
	)

	perCycleResettableMetrics := []resettableMetric{
		consideredJobs,
		fairShare,
		adjustedFairShare,
		actualShare,
		demand,
		cappedDemand,
		fairnessError,
		gangsConsidered,
		gangsScheduled,
		firstGangQueuePosition,
		lastGangQueuePosition,
		perQueueCycleTime,
		loopNumber,
		evictedJobs,
		evictedResources,
		spotPrice,
		queueWeight,
		rawQueueWeight,
	}

	allResettableMetrics := []resettableMetric{
		scheduledJobs,
		premptedJobs,
	}
	allResettableMetrics = append(allResettableMetrics, perCycleResettableMetrics...)

	return &cycleMetrics{
		leaderMetricsEnabled:      true,
		scheduledJobs:             scheduledJobs,
		preemptedJobs:             premptedJobs,
		consideredJobs:            consideredJobs,
		fairShare:                 fairShare,
		adjustedFairShare:         adjustedFairShare,
		actualShare:               actualShare,
		demand:                    demand,
		cappedDemand:              cappedDemand,
		queueWeight:               queueWeight,
		rawQueueWeight:            rawQueueWeight,
		fairnessError:             fairnessError,
		scheduleCycleTime:         scheduleCycleTime,
		gangsConsidered:           gangsConsidered,
		gangsScheduled:            gangsScheduled,
		firstGangQueuePosition:    firstGangQueuePosition,
		lastGangQueuePosition:     lastGangQueuePosition,
		perQueueCycleTime:         perQueueCycleTime,
		loopNumber:                loopNumber,
		evictedJobs:               evictedJobs,
		evictedResources:          evictedResources,
		spotPrice:                 spotPrice,
		perCycleResettableMetrics: perCycleResettableMetrics,
		allResettableMetrics:      allResettableMetrics,
		reconciliationCycleTime:   reconciliationCycleTime,
		cycleMetricAccessLock:     sync.Mutex{},
	}
}

func (m *cycleMetrics) enableLeaderMetrics() {
	m.leaderMetricsEnabled = true
}

func (m *cycleMetrics) disableLeaderMetrics() {
	m.cycleMetricAccessLock.Lock()
	defer m.cycleMetricAccessLock.Unlock()
	m.resetLeaderMetrics()
	m.leaderMetricsEnabled = false
}

func (m *cycleMetrics) resetLeaderMetrics() {
	for _, metric := range m.allResettableMetrics {
		metric.Reset()
	}
}

func (m *cycleMetrics) resetPerCycleMetrics() {
	for _, metric := range m.perCycleResettableMetrics {
		metric.Reset()
	}
}

func (m *cycleMetrics) ReportScheduleCycleTime(cycleTime time.Duration) {
	m.scheduleCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportReconcileCycleTime(cycleTime time.Duration) {
	m.reconciliationCycleTime.Observe(float64(cycleTime.Milliseconds()))
}

func (m *cycleMetrics) ReportSchedulerResult(result scheduling.SchedulerResult) {
	m.cycleMetricAccessLock.Lock()
	defer m.cycleMetricAccessLock.Unlock()
	m.resetPerCycleMetrics()
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
			m.queueWeight.WithLabelValues(pool, queue).Set(queueContext.Weight)
			m.rawQueueWeight.WithLabelValues(pool, queue).Set(queueContext.RawWeight)
		}
		m.fairnessError.WithLabelValues(pool).Set(schedContext.FairnessError())
		m.spotPrice.WithLabelValues(pool).Set(schedContext.SpotPrice)
	}

	for _, jobCtx := range result.ScheduledJobs {
		m.scheduledJobs.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}

	for _, jobCtx := range result.PreemptedJobs {
		m.preemptedJobs.WithLabelValues(jobCtx.Job.Queue(), jobCtx.PriorityClassName).Inc()
	}

	for pool, schedulingStats := range result.PerPoolSchedulingStats {
		for queue, s := range schedulingStats.StatsPerQueue {
			m.gangsConsidered.WithLabelValues(pool, queue).Set(float64(s.GangsConsidered))
			m.gangsScheduled.WithLabelValues(pool, queue).Set(float64(s.GangsScheduled))
			m.firstGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.FirstGangConsideredQueuePosition))
			m.lastGangQueuePosition.WithLabelValues(pool, queue).Set(float64(s.LastGangScheduledQueuePosition))
			m.perQueueCycleTime.WithLabelValues(pool, queue).Set(float64(s.Time.Milliseconds()))
		}

		m.loopNumber.WithLabelValues(pool).Set(float64(schedulingStats.LoopNumber))

		for queue, s := range schedulingStats.EvictorResult.GetStatsPerQueue() {
			m.evictedJobs.WithLabelValues(pool, queue).Set(float64(s.EvictedJobCount))

			for _, r := range s.EvictedResources.GetResources() {
				m.evictedResources.WithLabelValues(pool, queue, r.Name).Set(float64(r.RawValue))
			}
		}
	}
}

func (m *cycleMetrics) describe(ch chan<- *prometheus.Desc) {
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Describe(ch)
		m.preemptedJobs.Describe(ch)
		m.consideredJobs.Describe(ch)
		m.fairShare.Describe(ch)
		m.adjustedFairShare.Describe(ch)
		m.actualShare.Describe(ch)
		m.fairnessError.Describe(ch)
		m.demand.Describe(ch)
		m.cappedDemand.Describe(ch)
		m.queueWeight.Describe(ch)
		m.rawQueueWeight.Describe(ch)
		m.scheduleCycleTime.Describe(ch)
		m.gangsConsidered.Describe(ch)
		m.gangsScheduled.Describe(ch)
		m.firstGangQueuePosition.Describe(ch)
		m.lastGangQueuePosition.Describe(ch)
		m.perQueueCycleTime.Describe(ch)
		m.loopNumber.Describe(ch)
		m.evictedJobs.Describe(ch)
		m.evictedResources.Describe(ch)
		m.spotPrice.Describe(ch)
	}

	m.reconciliationCycleTime.Describe(ch)
}

func (m *cycleMetrics) collect(ch chan<- prometheus.Metric) {
	m.cycleMetricAccessLock.Lock()
	defer m.cycleMetricAccessLock.Unlock()
	if m.leaderMetricsEnabled {
		m.scheduledJobs.Collect(ch)
		m.preemptedJobs.Collect(ch)
		m.consideredJobs.Collect(ch)
		m.fairShare.Collect(ch)
		m.adjustedFairShare.Collect(ch)
		m.actualShare.Collect(ch)
		m.fairnessError.Collect(ch)
		m.demand.Collect(ch)
		m.cappedDemand.Collect(ch)
		m.rawQueueWeight.Collect(ch)
		m.queueWeight.Collect(ch)
		m.scheduleCycleTime.Collect(ch)
		m.gangsConsidered.Collect(ch)
		m.gangsScheduled.Collect(ch)
		m.firstGangQueuePosition.Collect(ch)
		m.lastGangQueuePosition.Collect(ch)
		m.perQueueCycleTime.Collect(ch)
		m.loopNumber.Collect(ch)
		m.evictedJobs.Collect(ch)
		m.evictedResources.Collect(ch)
		m.spotPrice.Collect(ch)
	}

	m.reconciliationCycleTime.Collect(ch)
}
