package logging

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// implements zerolog.Hook
type prometheusHook struct {
	counters map[zerolog.Level]prometheus.Counter
}

// NewPrometheusHook creates and registers Prometheus counters for each log level.
func NewPrometheusHook() *prometheusHook {
	counters := make(map[zerolog.Level]prometheus.Counter)

	for _, level := range []zerolog.Level{
		zerolog.DebugLevel,
		zerolog.InfoLevel,
		zerolog.WarnLevel,
		zerolog.ErrorLevel,
	} {
		counter := prometheus.NewCounter(prometheus.CounterOpts{
			Name: "log_messages",
			Help: "Total number of log lines logged by level",
			ConstLabels: prometheus.Labels{
				"level": level.String(),
			},
		})
		// Register the counter with Prometheus.
		prometheus.MustRegister(counter)
		counters[level] = counter
	}
	return &prometheusHook{counters: counters}
}

func (h *prometheusHook) Run(_ *zerolog.Event, level zerolog.Level, _ string) {
	if counter, ok := h.counters[level]; ok {
		counter.Inc()
	}
}
