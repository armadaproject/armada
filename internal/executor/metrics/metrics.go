package metrics

import (
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
