package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var requestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "lookout_request_duration_ms",
		Help:    "Request duration in milliseconds",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
	},
	[]string{"user", "endpoint"},
)

var requestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "lookout_requests_total",
		Help: "Number of requests handled, by response status code",
	},
	[]string{"user", "endpoint", "method", "code"},
)

func RecordRequestDuration(user, endpoint string, duration float64) {
	requestDuration.
		With(map[string]string{"user": user, "endpoint": endpoint}).
		Observe(duration)
}

func RecordRequest(user, endpoint, method string, code int) {
	requestsTotal.
		With(map[string]string{"user": user, "endpoint": endpoint, "method": method, "code": strconv.Itoa(code)}).
		Inc()
}
