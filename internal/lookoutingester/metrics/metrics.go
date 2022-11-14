package metrics

import (
	"github.com/G-Research/armada/internal/common/ingest/metrics"
)

var m = metrics.NewMetrics(metrics.ArmadaLookoutIngesterMetricsPrefix)

func Get() *metrics.Metrics {
	return m
}
