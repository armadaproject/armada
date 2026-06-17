package testsuite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	DefaultEvidenceDir         = ".sisyphus/evidence"
	traceEvidenceStrictModeEnv = "OTEL_TRACE_EVIDENCE_STRICT"
)

type TraceEvidence struct {
	TraceIDs     []string       `json:"trace_ids"`
	Spans        []SpanEvidence `json:"spans"`
	ServiceNames []string       `json:"service_names"`
	TotalSpans   int            `json:"total_spans"`
}

type SpanEvidence struct {
	TraceID     string            `json:"trace_id"`
	SpanID      string            `json:"span_id"`
	SpanName    string            `json:"span_name"`
	ServiceName string            `json:"service_name"`
	Status      string            `json:"status"`
	Attributes  map[string]string `json:"attributes"`
}

type InMemoryTracerCollector struct {
	spanRecorder   *tracetest.SpanRecorder
	tracerProvider *sdktrace.TracerProvider
}

func NewInMemoryTracerCollector() *InMemoryTracerCollector {
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spanRecorder),
	)

	return &InMemoryTracerCollector{
		spanRecorder:   spanRecorder,
		tracerProvider: tracerProvider,
	}
}

// NewInMemoryTraceCollector is retained as a backward-compatible alias.
// Deprecated: use NewInMemoryTracerCollector instead.
func NewInMemoryTraceCollector() *InMemoryTracerCollector {
	return NewInMemoryTracerCollector()
}

func (c *InMemoryTracerCollector) TracerProvider() *sdktrace.TracerProvider {
	return c.tracerProvider
}

func (c *InMemoryTracerCollector) Shutdown(ctx context.Context) error {
	return c.tracerProvider.Shutdown(ctx)
}

func (c *InMemoryTracerCollector) CollectEvidence() (*TraceEvidence, error) {
	spans := c.spanRecorder.Ended()
	traceIDs := make(map[string]struct{})
	serviceNames := make(map[string]struct{})
	spanEvidence := make([]SpanEvidence, 0, len(spans))

	for _, span := range spans {
		traceID := span.SpanContext().TraceID().String()
		spanID := span.SpanContext().SpanID().String()
		serviceName := serviceNameForSpan(span)

		traceIDs[traceID] = struct{}{}
		if serviceName != "" {
			serviceNames[serviceName] = struct{}{}
		}

		spanEvidence = append(spanEvidence, SpanEvidence{
			TraceID:     traceID,
			SpanID:      spanID,
			SpanName:    span.Name(),
			ServiceName: serviceName,
			Status:      spanStatus(span.Status().Code),
			Attributes:  evidenceAttributes(span.Attributes()),
		})
	}

	return &TraceEvidence{
		TraceIDs:     sortedStringSet(traceIDs),
		Spans:        spanEvidence,
		ServiceNames: sortedStringSet(serviceNames),
		TotalSpans:   len(spans),
	}, nil
}

func WriteTraceEvidenceFile(evidence *TraceEvidence, path string) error {
	if evidence == nil {
		return fmt.Errorf("trace evidence must not be nil")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func ValidateTraceEvidence(evidence *TraceEvidence) error {
	if evidence == nil {
		return fmt.Errorf("trace evidence must not be nil")
	}
	if evidence.TotalSpans == 0 {
		return fmt.Errorf("trace evidence must contain at least one span")
	}
	if len(evidence.TraceIDs) == 0 {
		return fmt.Errorf("trace evidence must contain at least one trace id")
	}
	if len(evidence.ServiceNames) == 0 {
		return fmt.Errorf("trace evidence must contain at least one service name")
	}
	return nil
}

func IsStrictModeEnabled() bool {
	return strings.EqualFold(os.Getenv(traceEvidenceStrictModeEnv), "true")
}

func serviceNameForSpan(span sdktrace.ReadOnlySpan) string {
	if serviceName := serviceNameFromResource(span.Resource()); serviceName != "" {
		return serviceName
	}

	for _, attr := range span.Attributes() {
		if attr.Key == semconv.ServiceNameKey {
			return attr.Value.AsString()
		}
	}

	return span.InstrumentationScope().Name
}

func serviceNameFromResource(resource *resource.Resource) string {
	if resource == nil {
		return ""
	}

	for _, attr := range resource.Attributes() {
		if attr.Key == semconv.ServiceNameKey {
			return attr.Value.AsString()
		}
	}

	return ""
}

func evidenceAttributes(attributes []attribute.KeyValue) map[string]string {
	result := make(map[string]string)
	for _, attr := range attributes {
		key := string(attr.Key)
		if isEvidenceAttribute(key) {
			result[key] = attr.Value.AsString()
		}
	}
	return result
}

func isEvidenceAttribute(key string) bool {
	return strings.HasPrefix(key, "rpc.") ||
		strings.HasPrefix(key, "http.") ||
		strings.HasPrefix(key, "service.")
}

func spanStatus(code codes.Code) string {
	switch code {
	case codes.Ok:
		return "ok"
	case codes.Error:
		return "error"
	default:
		return "unset"
	}
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
