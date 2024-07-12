package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/armadaproject/armada/internal/common/ingest/metrics"
)

// Lookout ingester specific metrics
var avRowChangeTimeHist = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    metrics.ArmadaLookoutIngesterV2MetricsPrefix + "average_row_change_time",
		Help:    "Average time take in microseconds to change one database row",
		Buckets: []float64{1, 10, 100, 1000, 10000, 1e5, 1e6, 1e7, 1e8},
	},
)

var rowsChangedCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: metrics.ArmadaLookoutIngesterV2MetricsPrefix + "row_change_time",
		Help: "Number of rows changed in the database",
	},
	[]string{"table", "operation"},
)

type Metrics struct {
	*metrics.Metrics
}

var m = &Metrics{
	metrics.NewMetrics(metrics.ArmadaLookoutIngesterV2MetricsPrefix),
}

func Get() *Metrics {
	return m
}

func (m *Metrics) RecordAvRowChangeTime(duration float64) {
	avRowChangeTimeHist.Observe(duration)
}

func (m *Metrics) RecordRowsChange(table string, operation metrics.DBOperation, numRows int) {
	rowsChangedCounter.
		With(map[string]string{"table": table, "operation": string(operation)}).
		Add(float64(numRows))
}
