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
)

var jobFailureCategoryTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: ArmadaExecutorMetricsPrefix + "job_failure_category_total",
		Help: "Total number of job failures by failure category and subcategory",
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

// RecordJobFailure increments the per-category failure counter. Should be
// called from paths that commit to emitting a JobFailedEvent — retryable
// pod issues that emit a ReturnLease instead must not call this.
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
