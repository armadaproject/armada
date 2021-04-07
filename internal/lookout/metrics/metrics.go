package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const MetricPrefix = "lookout_"

var RequestsTotalCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: MetricPrefix + "requests_total",
	Help: "Total number of incoming requests",
})

var dbOpenConnectionsDesc = prometheus.NewDesc(
	MetricPrefix+"db_open_connections",
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

type LookoutDbCollector struct{}

func ExposeLookoutMetrics() {
	prometheus.MustRegister(RequestsTotalCounter)
}

func (c *LookoutDbCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- dbOpenConnectionsDesc
	desc <- dbOpenConnectionsUtilizationDesc
}

func (c *LookoutDbCollector) Collect(metrics chan<- prometheus.Metric) {

}
