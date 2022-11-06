package metrics

import (
	"github.com/G-Research/armada/internal/common/ingest/metrics"
)

var m = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "armada_scheduler_ingester_")

func Get() *metrics.Metrics {
	return m
}
