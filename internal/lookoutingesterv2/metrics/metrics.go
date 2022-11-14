package metrics

import (
	"github.com/G-Research/armada/internal/common/ingest/metrics"
)

type (
	DBOperation        string
	PulsarMessageError string
)

var m = metrics.NewMetrics(metrics.ArmadaLookoutIngesterV2MetricsPrefix)

func Get() *metrics.Metrics {
	return m
}
