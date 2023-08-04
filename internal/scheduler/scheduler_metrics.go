package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
)

const NAMESPACE = "armada"
const SUBSYSTEM = "scheduler"

type SchedulerMetrics struct {
	CycleTime          prometheus.Gauge
	CycleTimeHistogram prometheus.Histogram
}

func NewSchedulerMetrics() *SchedulerMetrics {
	cycleTime := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   NAMESPACE,
			Subsystem:   SUBSYSTEM,
			Name:        "last_cycle_time",
			Help:        "Time to complete most recent scheduling cycle.",
			ConstLabels: map[string]string{},
		})

	cycleTimeHistogram := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: NAMESPACE,
			Subsystem: SUBSYSTEM,
			Name:      "cycle_times",
			Help:      "Histogram of cycle times observed.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		},
	)

	prometheus.MustRegister(cycleTime)
	prometheus.MustRegister(cycleTimeHistogram)

	return &SchedulerMetrics{
		CycleTime:          cycleTime,
		CycleTimeHistogram: cycleTimeHistogram,
	}

}

func (metrics *SchedulerMetrics) ReportCycleTime(cycleTime float64) {
	metrics.CycleTime.Add(cycleTime)
	metrics.CycleTimeHistogram.Observe(cycleTime)
}
