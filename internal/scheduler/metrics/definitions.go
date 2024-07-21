package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MetricsPrefix         = "armada_scheduler_"
	PoolLabel             = "pool"
	QueueLabel            = "queue"
	PriorityClassLabel    = "priority_class"
	ClusterLabel          = "cluster"
	ErrorCategoryLabel    = "category"
	ErrorSubcategoryLabel = "subcategory"
	StateLabel            = "state"
	PriorStateLabel       = "priorState"
	ResourceLabel         = "resource"
)

var (
	QueueAndPoolLabels          = []string{QueueLabel, PoolLabel}
	QueueAndPriorityClassLabels = []string{QueueLabel, PriorityClassLabel}
	ErrorCategorylabels         = []string{QueueLabel, ClusterLabel, ErrorCategoryLabel, ErrorSubcategoryLabel}
	StateLabels                 = []string{QueueLabel, ClusterLabel, StateLabel, PriorStateLabel}
	ResourceStateLabels         = []string{QueueLabel, ClusterLabel, StateLabel, PriorStateLabel, ResourceLabel}
)

var (
	scheduledJobsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: MetricsPrefix + "scheduled_jobs",
			Help: "Number of events scheduled",
		},
		QueueAndPriorityClassLabels,
	)

	premptedJobsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: MetricsPrefix + "preempted_jobs",
			Help: "Number of jobs preempted",
		},
		QueueAndPriorityClassLabels,
	)

	consideredJobsMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "considered_jobs",
			Help: "Number of jobs considered",
		},
		QueueAndPoolLabels,
	)

	fairShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "fair_share",
			Help: "Fair share of each queue",
		},
		QueueAndPoolLabels,
	)

	adjustedFairShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "adjusted_fair_share",
			Help: "Adjusted Fair share of each queue",
		},
		QueueAndPoolLabels,
	)

	actualShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "actual_share",
			Help: "Actual Fair share of each queue",
		},
		QueueAndPoolLabels,
	)

	demandMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "demand",
			Help: "Actual Fair share of each queue",
		},
		QueueAndPoolLabels,
	)

	cappedDemandMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "capped_demand",
			Help: "Capped Demand of each queue and pool.  This differs from demand in that it limits demand by scheduling constraints",
		},
		QueueAndPoolLabels,
	)

	fairnessErrorMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: MetricsPrefix + "fairness_error",
			Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
		},
		[]string{PoolLabel},
	)

	scheduleCycleTimeMetric = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    MetricsPrefix + "schedule_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	reconciliationCycleTimeMetric = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    MetricsPrefix + "reconciliation_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	completedRunDurationsMetric = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    MetricsPrefix + "job_run_completed_duration_seconds",
			Help:    "Time",
			Buckets: prometheus.ExponentialBuckets(2, 2, 20),
		},
		[]string{QueueLabel},
	)

	jobStateSecondsMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: MetricsPrefix + "job_state_seconds",
			Help: "Resource Seconds spend in different states",
		},
		StateLabels,
	)

	jobStateResourceSecondsMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: MetricsPrefix + "job_state_resource_seconds",
			Help: "Resource Seconds spend in different states",
		},
		ResourceStateLabels,
	)

	jobErrorsMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: MetricsPrefix + "job_error_classification",
			Help: "Failed jobs ey error classification",
		},
		ErrorCategorylabels,
	)
)
