package observability

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var traceExportSpans = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "armada_otel_export_spans_total",
	Help: "Spans in completed exporter calls by result. Errors do not establish backend loss; queue drops are not counted.",
}, []string{"result"})

// meteredTraceExporter observes the existing export path without adding retries
// or changing its errors. The SDK still owns batching, timeouts and shutdown.
type meteredTraceExporter struct {
	sdktrace.SpanExporter
}

func (e *meteredTraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.SpanExporter.ExportSpans(ctx, spans)
	result := "success"
	if err != nil {
		result = "error"
	}
	traceExportSpans.WithLabelValues(result).Add(float64(len(spans)))
	return err
}
