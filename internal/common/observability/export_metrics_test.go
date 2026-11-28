package observability

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMeteredTraceExporter(t *testing.T) {
	exportError := errors.New("collector unavailable")
	for _, tc := range []struct {
		result string
		err    error
	}{
		{"success", nil},
		{"error", exportError},
	} {
		t.Run(tc.result, func(t *testing.T) {
			successBefore := testutil.ToFloat64(traceExportSpans.WithLabelValues("success"))
			errorBefore := testutil.ToFloat64(traceExportSpans.WithLabelValues("error"))
			inner := &resultTraceExporter{err: tc.err}
			exporter := &meteredTraceExporter{SpanExporter: inner}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			spans := []sdktrace.ReadOnlySpan{
				tracetest.SpanStub{Name: "first"}.Snapshot(),
				tracetest.SpanStub{Name: "second"}.Snapshot(),
			}
			err := exporter.ExportSpans(ctx, spans)
			require.Equal(t, tc.err, err)
			assert.Equal(t, context.Canceled, inner.ctx.Err(), "preserve exporter cancellation")
			assert.Equal(t, spans, inner.spans)
			wantSuccess, wantError := successBefore, errorBefore
			if tc.err == nil {
				wantSuccess += 2
			} else {
				wantError += 2
			}
			assert.Equal(t, wantSuccess, testutil.ToFloat64(traceExportSpans.WithLabelValues("success")))
			assert.Equal(t, wantError, testutil.ToFloat64(traceExportSpans.WithLabelValues("error")))
			require.NoError(t, exporter.Shutdown(ctx))
			assert.True(t, inner.shutdown)
		})
	}
}

type resultTraceExporter struct {
	err      error
	ctx      context.Context
	spans    []sdktrace.ReadOnlySpan
	shutdown bool
}

func (e *resultTraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.ctx, e.spans = ctx, spans
	return e.err
}

func (e *resultTraceExporter) Shutdown(context.Context) error {
	e.shutdown = true
	return nil
}
