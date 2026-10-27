package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const ArmadaExecutorMetricsPrefix = "armada_executor_"

const (
	failureCategoryLabel    = "failure_category"
	failureSubcategoryLabel = "failure_subcategory"
	poolLabel               = "pool"
)

var jobFailureCategoryTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: ArmadaExecutorMetricsPrefix + "job_failure_category_total",
		Help: "Total number of job run failures by failure category and subcategory. " +
			"Includes retryable failures whose lease is returned for rescheduling, " +
			"so a single job can contribute multiple increments.",
	},
	[]string{failureCategoryLabel, failureSubcategoryLabel},
)

var jobFailureRuleEvaluationDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: ArmadaExecutorMetricsPrefix + "job_failure_rule_evaluation_duration_seconds",
		Help: "Duration of evaluating a single classification rule against a pod, " +
			"labeled by the rule's category and subcategory. Observed for every " +
			"rule evaluation regardless of whether it matched.",
		Buckets: []float64{
			0.00001, 0.0001, 0.0005,
			0.001, 0.005, 0.01, 0.05,
			0.1, 0.25,
		},
	},
	[]string{failureCategoryLabel, failureSubcategoryLabel},
)

// The pod termination metrics are labelled by pool only. Cluster-wide figures are sum() across
// pools, so there is one series per pool rather than one per queue.
var podForceDeletedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: ArmadaExecutorMetricsPrefix + "pod_force_deleted_total",
		Help: "Total number of force delete escalations issued by the executor, by pool.",
	},
	[]string{poolLabel},
)

// Bucketed finely between 10s and 2m, which is the range Armada's own timers act in: the force
// delete escalates at podKillTimeout past the deadline and is then gated on the 2 minute
// repeat-deletion debounce. Anything past 5 minutes is a stuck pod whose exact overrun does not
// matter, so it falls in +Inf.
var podTerminationOverdueSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    ArmadaExecutorMetricsPrefix + "pod_termination_overdue_seconds",
		Help:    "How far past its deletion deadline a pod survived, observed when it disappears from the cluster.",
		Buckets: []float64{0, 10, 15, 20, 30, 45, 60, 75, 90, 120, 300},
	},
	[]string{poolLabel},
)

var podTerminatedWithinGracePeriodTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: ArmadaExecutorMetricsPrefix + "pod_terminated_within_grace_period_total",
		Help: "Total number of pods that disappeared before their deletion deadline, by pool.",
	},
	[]string{poolLabel},
)

// Counts escalations issued, not pods that overran their grace period: the escalation is gated on the
// repeat-deletion debounce, so a pod that overran its deadline but disappeared during that window is
// never force deleted and never counted. RecordPodTerminationOverdue measures pod behaviour.
func RecordPodForceDeleted(pool string) {
	podForceDeletedTotal.WithLabelValues(pool).Inc()
}

// A pod that terminated inside its grace period - the healthy case - is counted separately rather
// than observed as a negative value, which would make the histogram's _sum non-monotonic and so
// break rate() over it.
func RecordPodTerminationOverdue(pool string, overdue time.Duration) {
	if overdue < 0 {
		podTerminatedWithinGracePeriodTotal.WithLabelValues(pool).Inc()
		return
	}
	podTerminationOverdueSeconds.WithLabelValues(pool).Observe(overdue.Seconds())
}

// RecordJobFailure increments the per-category failure counter. Should be
// called only after the failure event (JobFailedEvent, or ReturnLease for
// retryable pod issues) has been successfully reported, so failed sends do
// not inflate the counter.
//
// An empty category indicates no classification happened (e.g. the feature
// flag is off or the classifier is nil); in that case no metric is emitted.
// An empty subcategory is allowed and indicates a matched rule with no
// subcategory set; it produces an empty-string label value.
func RecordJobFailure(category, subcategory string) {
	if category == "" {
		return
	}
	jobFailureCategoryTotal.WithLabelValues(category, subcategory).Inc()
}

// RecordRuleEvaluationDuration records the time a single classification
// rule took to evaluate. Called for every rule regardless of match outcome.
// An empty category is a no-op to avoid an empty failure_category label.
func RecordRuleEvaluationDuration(category, subcategory string, duration time.Duration) {
	if category == "" {
		return
	}
	jobFailureRuleEvaluationDurationSeconds.WithLabelValues(category, subcategory).Observe(duration.Seconds())
}
