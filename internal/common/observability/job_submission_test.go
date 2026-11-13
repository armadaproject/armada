package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func sampledSubmitter() trace.SpanContext {
	return trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{
		"traceparent": "00-11111111111111111111111111111111-2222222222222222-01", "tracestate": "vendor=value,ot=th:0",
	}))
}

// Exercises the actual lifecycle wiring and OTLP payload, including a sampler
// which would otherwise suppress this job root.
func TestJobSubmissionLifecycle(t *testing.T) {
	requests := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		req := &collectortrace.ExportTraceServiceRequest{}
		if proto.Unmarshal(data, req) != nil {
			w.WriteHeader(400)
			return
		}
		requests <- req
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer collector.Close()
	cfg := testBootstrapConfig("job-test", "test", "instance")
	cfg.Enabled = true
	cfg.Exporter.Endpoint = collector.URL
	cfg.Exporter.Protocol = "http/protobuf"
	cfg.Traces.Sampler = "always_off"
	// Shared initialization keeps the SDK's ordinary IDs/sampler even if a
	// caller attempts to record a job submission.
	require.NoError(t, InitOTel(cfg))
	RecordJobSubmission("01ARZ3NDEKTSV4RRFFQ69G5FAV", "queue", "set", "annotation", sampledSubmitter(), time.Now(), time.Now())
	require.NoError(t, ShutdownOTel(context.Background()))
	select {
	case <-requests:
		t.Fatal("shared initializer enabled submission adapters")
	default:
	}
	require.NoError(t, InitOTelWithJobSubmissionSpans(cfg))
	t.Cleanup(func() { require.NoError(t, ShutdownOTel(context.Background())) })
	start := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	RecordJobSubmission("01ARZ3NDEKTSV4RRFFQ69G5FAV", "queue", "set", "annotation", sampledSubmitter(), start, start.Add(time.Second))
	// Existing spans still obey the configured sampler and receive random IDs.
	_, span := otel.Tracer("ordinary").Start(context.Background(), "ordinary")
	assert.False(t, span.IsRecording())
	assert.True(t, span.SpanContext().IsValid())
	span.End()
	require.NoError(t, ShutdownOTel(context.Background()))
	select {
	case req := <-requests:
		require.Len(t, req.ResourceSpans, 1)
		require.Len(t, req.ResourceSpans[0].ScopeSpans, 1)
		spans := req.ResourceSpans[0].ScopeSpans[0].Spans
		require.Len(t, spans, 1)
		assert.Equal(t, "01563e3ab5d3d6764c61efb99302bd5b", fmt.Sprintf("%x", spans[0].TraceId))
		assert.Equal(t, "4c61efb99302bd5b", fmt.Sprintf("%x", spans[0].SpanId))
		assert.Empty(t, spans[0].ParentSpanId)
		assert.Equal(t, "ot=th:0", spans[0].TraceState)
	case <-time.After(time.Second):
		t.Fatal("no job span exported")
	}
	// Disabled lifecycle must suppress roots even though the submitter is sampled.
	require.NoError(t, InitOTel(ObservabilityConfig{}))
	RecordJobSubmission("01ARZ3NDEKTSV4RRFFQ69G5FAV", "queue", "set", "annotation", sampledSubmitter(), start, start)
	select {
	case <-requests:
		t.Fatal("disabled provider exported a span")
	default:
	}
}

func TestJobSubmissionInvalidIDs(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithIDGenerator(JobSpanIDGenerator{}), sdktrace.WithSampler(JobSpanSampler{Fallback: sdktrace.AlwaysSample()}))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer func() { otel.SetTracerProvider(old); require.NoError(t, tp.Shutdown(context.Background())) }()
	for _, id := range []string{"bad", "00000000000000000000000000", "01ARZ3NDEK0000000000000000"} {
		RecordJobSubmission(id, "q", "s", "annotation", sampledSubmitter(), time.Now(), time.Now())
	}
	require.Empty(t, exporter.GetSpans())
}

func TestJobSpanRandomFallback(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithIDGenerator(JobSpanIDGenerator{}), sdktrace.WithSampler(JobSpanSampler{Fallback: sdktrace.AlwaysSample()}))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	ctx, root := tp.Tracer("test").Start(context.Background(), "root")
	defer root.End()
	_, child := tp.Tracer("test").Start(ctx, "child")
	defer child.End()
	assert.True(t, root.SpanContext().IsValid())
	assert.True(t, child.SpanContext().IsValid())
	assert.Equal(t, root.SpanContext().TraceID(), child.SpanContext().TraceID())
	assert.NotEqual(t, root.SpanContext().SpanID(), child.SpanContext().SpanID())
}

func TestJobIDsExemptFromCardinality(t *testing.T) {
	policy := NewDefaultSpanAttributePolicy()
	for i := 0; i < DefaultAttributeCardinalityLimit+10; i++ {
		attrs := []attribute.KeyValue{attribute.String("armada.job.id", fmt.Sprintf("job-%d", i))}
		require.Equal(t, attrs, policy.SanitizeForSpan(attrs))
		require.Empty(t, policy.ViolationsForSpan(attrs))
	}
	// Exempt identifiers consume no cardinality tracker entries.
	assert.Empty(t, policy.cardinality.seen)
}

// A stalled exporter must not backpressure submission once the bounded queue fills.
func TestJobSubmissionBoundedExport(t *testing.T) {
	exporter := &blockedJobExporter{started: make(chan struct{}, 1), release: make(chan struct{})}
	tp := sdktrace.NewTracerProvider(sdktrace.WithIDGenerator(JobSpanIDGenerator{}), sdktrace.WithSampler(JobSpanSampler{Fallback: sdktrace.NeverSample()}),
		sdktrace.WithBatcher(exporter, sdktrace.WithMaxExportBatchSize(maxExportBatch), sdktrace.WithMaxQueueSize(maxBatchQueue), sdktrace.WithExportTimeout(exportTimeout), sdktrace.WithBatchTimeout(batchTimeout)))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer func() {
		otel.SetTracerProvider(old)
		close(exporter.release)
		require.NoError(t, tp.Shutdown(context.Background()))
	}()
	emit := func() {
		RecordJobSubmission("01ARZ3NDEKTSV4RRFFQ69G5FAV", "queue", "set", "annotation", sampledSubmitter(), time.Now(), time.Now())
	}
	for range maxExportBatch {
		emit()
	}
	select {
	case <-exporter.started:
	case <-time.After(2 * time.Second):
		t.Fatal("export did not start")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range maxBatchQueue + maxExportBatch {
			emit()
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job spans blocked on a full export queue")
	}
}

type blockedJobExporter struct{ started, release chan struct{} }

func (e *blockedJobExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	select {
	case e.started <- struct{}{}:
	default:
	}
	select {
	case <-e.release:
		return errors.New("collector unavailable")
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (e *blockedJobExporter) Shutdown(context.Context) error { return nil }
