package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lookout_request_duration_ms",
			Help:    "Request duration in milliseconds",
			Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
		},
		[]string{"user", "endpoint"},
	)
)

func RecordRequestDuration(user, endpoint string, duration float64) {
	requestDuration.
		With(map[string]string{"user": user, "endpoint": endpoint}).
		Observe(duration)
}
