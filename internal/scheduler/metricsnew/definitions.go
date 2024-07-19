package metricsnew

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var scheduledJobsMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "events_processed",
		Help: "Number of events processed",
	},
	QueueAndPriorityClass)

var premptedJobsMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "preempted_jobs",
		Help: "Number of jobs preempted in the most recent round",
	},
	QueueAndPriorityClass)

var consideredJobsMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "preempted_jobs",
		Help: "Number of jobs considered in the most recent round",
	},
	QueueAndPool)

var fairShareMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "fair_share",
		Help: "Fair share of each queue",
	},
	QueueAndPool)

var adjustedFairShareMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "adjusted_fair_share",
		Help: "Adjusted Fair share of each queue",
	},
	QueueAndPool)

var actualShareMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "actual_share",
		Help: "Actual Fair share of each queue",
	},
	QueueAndPool)

var demandMetric = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "actual_share",
		Help: "Actual Fair share of each queue",
	},
	QueueAndPool)

var fairnessError = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: MetricsPrefix + "fairness_error",
		Help: "Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
	},
	QueueAndPool)
