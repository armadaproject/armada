package metrics

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/tools/metrics"
)

// This is largely needed as there is no way to customise the default bucket sizes unfortunately
// Similar approaches have been taken by kubernetes projects, such as:
// - https://github.com/kubernetes-sigs/controller-runtime/blob/139e0d728e115fdeb4718fb2de18eacb9fd85f91/pkg/metrics/client_go_adapter.go
var (
	requestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rest_client_request_duration_seconds",
			Help:    "Request latency in seconds. Broken down by verb and URL.",
			Buckets: []float64{.01, .05, .1, .2, .3, .4, .5, .6, .8, 1, 1.25, 1.5, 1.75, 2, 3, 4, 5, 10},
		},
		[]string{"verb", "url"},
	)

	requestResult = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rest_client_requests_total",
			Help: "Number of HTTP requests, partitioned by status code, method, and host.",
		},
		[]string{"code", "method", "host"},
	)

	rateLimiterLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "rest_client_rate_limiter_duration_seconds",
			Help: "Client side rate limiter latency in seconds. Broken down by verb and URL.",
		},
		[]string{"verb", "url"},
	)
)

func init() {
	prometheus.MustRegister(requestLatency)
	prometheus.MustRegister(requestResult)
	prometheus.MustRegister(rateLimiterLatency)
	requestLatencyAdapter := &latencyAdapter{m: requestLatency}
	rateLimiterLatencyAdapter := &latencyAdapter{m: rateLimiterLatency}
	resultAdapter := &resultAdapter{requestResult}
	opts := metrics.RegisterOpts{
		RequestLatency:     requestLatencyAdapter,
		RequestResult:      resultAdapter,
		RateLimiterLatency: rateLimiterLatencyAdapter,
	}
	metrics.Register(opts)
}

type latencyAdapter struct {
	m *prometheus.HistogramVec
}

func (l *latencyAdapter) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	l.m.WithLabelValues(verb, u.String()).Observe(latency.Seconds())
}

type resultAdapter struct {
	m *prometheus.CounterVec
}

func (r *resultAdapter) Increment(ctx context.Context, code, method, host string) {
	r.m.WithLabelValues(code, method, host).Inc()
}
