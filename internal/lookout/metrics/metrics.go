package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const MetricPrefix = "lookout_"

const StatusOk = "ok"
const StatusError = "error"

var requestsDurationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: MetricPrefix + "request_duration_seconds",
	Help: "Duration of each request",
}, []string{"endpoint", "status"})

var dbOpenConnectionsDesc = prometheus.NewDesc(
	MetricPrefix+"db_open_connections_total",
	"Number of open connections to database",
	nil,
	nil,
)

var dbOpenConnectionsUtilizationDesc = prometheus.NewDesc(
	MetricPrefix+"db_open_connections_utilization",
	"Percentage of connections used over total allowed connections to database",
	nil,
	nil,
)

type LookoutCollector interface {
	Describe(desc chan<- *prometheus.Desc)
	Collect(metrics chan<- prometheus.Metric)
	RecordRequestDuration(duration time.Duration, endpoint string, status string)
}

type LookoutApiCollector struct {
	lookoutDbMetricsProvider LookoutDbMetricsProvider
}

func ExposeLookoutMetrics(lookoutDbMetricsProvider LookoutDbMetricsProvider) LookoutCollector {
	collector := &LookoutApiCollector{
		lookoutDbMetricsProvider: lookoutDbMetricsProvider,
	}
	prometheus.MustRegister(collector)
	prometheus.MustRegister(requestsDurationHist)
	return collector
}

func (c *LookoutApiCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- dbOpenConnectionsDesc
	desc <- dbOpenConnectionsUtilizationDesc
}

func (c *LookoutApiCollector) Collect(metrics chan<- prometheus.Metric) {
	nOpenConnections := c.lookoutDbMetricsProvider.GetOpenConnections()
	openConnectionsUtilization := c.lookoutDbMetricsProvider.GetOpenConnectionsUtilization()

	metrics <- prometheus.MustNewConstMetric(dbOpenConnectionsDesc, prometheus.GaugeValue, float64(nOpenConnections))
	metrics <- prometheus.MustNewConstMetric(dbOpenConnectionsUtilizationDesc, prometheus.GaugeValue, openConnectionsUtilization)
}

func (c *LookoutApiCollector) RecordRequestDuration(duration time.Duration, endpoint string, status string) {
	requestsDurationHist.WithLabelValues(endpoint, status).Observe(duration.Seconds())
}
