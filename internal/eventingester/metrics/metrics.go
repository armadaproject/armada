package metrics

import (
	"github.com/G-Research/armada/internal/common/ingester/metrics"
)

var m = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix)

func Get() *metrics.Metrics {
	return m
}
