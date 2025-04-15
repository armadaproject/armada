package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
)

var m = metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix)

func Get() *metrics.Metrics {
	return m
}

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    metrics.ArmadaEventIngesterMetricsPrefix + "write_duration_ms",
		Help:    "Duration of write to redis in milliseconds",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
	},
	[]string{"redis", "result"},
)

func RecordWriteDuration(redisName, result string, duration time.Duration) {
	requestDuration.
		With(map[string]string{"redis": redisName, "result": result}).
		Observe(float64(duration.Milliseconds()))
}
