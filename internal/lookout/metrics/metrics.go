package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const MetricPrefix = "lookout_"

var dbOpenConnectionsDesc = prometheus.NewDesc(
	MetricPrefix+"db_open_connections_total",
	"Number of open connections to database",
	nil,
	nil,
)

var dbOpenConnectionsUtilizationDesc = prometheus.NewDesc(
	MetricPrefix+"db_open_connections_utilisation",
	"Percentage of connections used over total allowed connections to database",
	nil,
	nil,
)

type LookoutCollector interface {
	Describe(desc chan<- *prometheus.Desc)
	Collect(metrics chan<- prometheus.Metric)
}

type LookoutApiCollector struct {
	lookoutDbMetricsProvider LookoutDbMetricsProvider
}

func ExposeLookoutMetrics(lookoutDbMetricsProvider LookoutDbMetricsProvider) LookoutCollector {
	collector := &LookoutApiCollector{
		lookoutDbMetricsProvider: lookoutDbMetricsProvider,
	}
	prometheus.MustRegister(collector)
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
