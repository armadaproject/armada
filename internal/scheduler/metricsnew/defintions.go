package metricsnew

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	MetricsPrefix = "armada_scheduler_"
)

var (
	QueueAndPool          = []string{"queue", "pool"}
	QueueAndPriorityClass = []string{"queue", "priority_class"}
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

var demandPerQueueDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "demand"),
	"Demand of each queue and pool.",
	[]string{
		"queue",
		"pool",
	}, nil,
)

var fairnessErrorDesc = prometheus.NewDesc(
	fmt.Sprintf("%s_%s_%s", NAMESPACE, SUBSYSTEM, "fairness_error"),
	"Cumulative delta between adjusted fair share and actual share for all users who are below their fair share",
	[]string{
		"pool",
	}, nil,
)

func NewMetrics(prefix string) *Metrics {
	dbErrorsCounterOpts := prometheus.CounterOpts{
		Name: prefix + "db_errors",
		Help: "Number of database errors grouped by database operation",
	}
	pulsarMessageErrorOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_message_errors",
		Help: "Number of Pulsar message errors grouped by error type",
	}
	pulsarConnectionErrorOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_connection_errors",
		Help: "Number of Pulsar connection errors",
	}
	pulsarMessagesProcessedOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_messages_processed",
		Help: "Number of pulsar messages processed",
	}
	eventsProcessedOpts := prometheus.CounterOpts{
		Name: prefix + "events_processed",
		Help: "Number of events processed",
	}

	return &Metrics{}
}
