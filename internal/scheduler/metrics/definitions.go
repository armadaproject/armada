package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	Prefix                = "armada_scheduler_"
	PoolLabel             = "pool"
	QueueLabel            = "queue"
	PriorityClassLabel    = "priority_class"
	NodeLabel             = "node"
	ClusterLabel          = "cluster"
	ErrorCategoryLabel    = "category"
	ErrorSubcategoryLabel = "subcategory"
	StateLabel            = "state"
	PriorStateLabel       = "priorState"
	ResourceLabel         = "resource"
)

var (
	PoolAndQueueLabels          = []string{PoolLabel, QueueLabel}
	QueueAndPriorityClassLabels = []string{QueueLabel, PriorityClassLabel}
	QueueErrorCategorylabels    = []string{QueueLabel, ErrorCategoryLabel, ErrorSubcategoryLabel}
	NodeErrorCategorylabels     = []string{NodeLabel, ClusterLabel, ErrorCategoryLabel, ErrorSubcategoryLabel}
	QueueStateLabels            = []string{QueueLabel, StateLabel, PriorStateLabel}
	NodeStateLabels             = []string{NodeLabel, ClusterLabel, StateLabel, PriorStateLabel}
	QueueResourceStateLabels    = []string{QueueLabel, StateLabel, PriorStateLabel, ResourceLabel}
	NodeResourceStateLabels     = []string{NodeLabel, ClusterLabel, StateLabel, PriorStateLabel, ResourceLabel}
)

var (
	scheduledJobsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "scheduled_jobs",
			Help: "Number of events scheduled",
		},
		QueueAndPriorityClassLabels,
	)

	premptedJobsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "preempted_jobs",
			Help: "Number of jobs preempted",
		},
		QueueAndPriorityClassLabels,
	)

	consideredJobsMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "considered_jobs",
			Help: "Number of jobs considered",
		},
		PoolAndQueueLabels,
	)

	fairShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "fair_share",
			Help: "Fair share of each queue",
		},
		PoolAndQueueLabels,
	)

	adjustedFairShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "adjusted_fair_share",
			Help: "Adjusted Fair share of each queue",
		},
		PoolAndQueueLabels,
	)

	actualShareMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "actual_share",
			Help: "Actual Fair share of each queue",
		},
		PoolAndQueueLabels,
	)

	demandMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "demand",
			Help: "Actual Fair share of each queue",
		},
		PoolAndQueueLabels,
	)

	cappedDemandMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "capped_demand",
			Help: "Capped Demand of each queue and pool.  This differs from demand in that it limits demand by scheduling constraints",
		},
		PoolAndQueueLabels,
	)

	fairnessErrorMetric = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: Prefix + "fairness_error",
			Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
		},
		[]string{PoolLabel},
	)

	scheduleCycleTimeMetric = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    Prefix + "schedule_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	reconciliationCycleTimeMetric = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    Prefix + "reconciliation_cycle_times",
			Help:    "Cycle time when in a scheduling round.",
			Buckets: prometheus.ExponentialBuckets(10.0, 1.1, 110),
		},
	)

	completedRunDurationsMetric = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    Prefix + "job_run_completed_duration_seconds",
			Help:    "Time",
			Buckets: prometheus.ExponentialBuckets(2, 2, 20),
		},
		[]string{QueueLabel},
	)

	queueJobStateSecondsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "queue_job_state_seconds",
			Help: "Resource-seconds spend in different states at queue level",
		},
		QueueStateLabels,
	)

	nodeJobStateSecondsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "node_job_state_seconds",
			Help: "Resource-seconds spend in different states at node level",
		},
		NodeStateLabels,
	)

	queuejobStateResourceSecondsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "queue_job_state_resource_seconds",
			Help: "Resource Seconds spend in different states at queue level",
		},
		QueueResourceStateLabels,
	)

	nodeStateResourceSecondsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "node_job_state_resource_seconds",
			Help: "Resource Seconds spend in different states at node level",
		},
		NodeStateLabels,
	)

	queueJobErrorsMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: Prefix + "job_error_classification",
			Help: "Failed jobs ey error classification",
		},
		NodeResourceStateLabels,
	)
)
