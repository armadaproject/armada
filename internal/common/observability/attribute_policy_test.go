package observability

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestSpanAttributeAllowListPreservesStandardKeys(t *testing.T) {
	policy := NewDefaultSpanAttributePolicy()

	attrs := []attribute.KeyValue{
		attribute.String("rpc.system.name", "grpc"),
		attribute.String("http.method", "GET"),
		attribute.Int("http.status_code", 200),
		attribute.String("net.peer.name", "localhost"),
		attribute.String("server.address", "armada.local"),
		attribute.String("service.name", "server"),
		attribute.String("trace_id", "abc"),
		attribute.String("span_id", "def"),
	}

	sanitized := policy.SanitizeForSpan(attrs)
	require.Len(t, sanitized, len(attrs))

	for i := range attrs {
		assert.Equal(t, attrs[i], sanitized[i])
	}
}

func TestSpanAttributePolicyDropsPII(t *testing.T) {
	policy := NewDefaultSpanAttributePolicy()

	attrs := []attribute.KeyValue{
		attribute.String("user_email", "user@example.com"),
		attribute.String("password", "super-secret"),
		attribute.String("request_body", `{"sensitive":true}`),
		attribute.String("armada.custom_token_field", "token-value"),
		attribute.String("rpc.system.name", "grpc"),
	}

	sanitized := policy.SanitizeForSpan(attrs)

	assert.Equal(t, attribute.String("user_email", AttributeRedactedValue), sanitized[0])
	assert.Equal(t, attribute.String("password", AttributeRedactedValue), sanitized[1])
	assert.Equal(t, attribute.String("request_body", AttributeRedactedValue), sanitized[2])
	assert.Equal(t, attribute.String("armada.custom_token_field", AttributeRedactedValue), sanitized[3])
	assert.Equal(t, attribute.String("rpc.system.name", "grpc"), sanitized[4])
}

func TestSpanAttributePolicyBoundsCardinality(t *testing.T) {
	policy := NewDefaultSpanAttributePolicy()

	customKey := "armada.user_agent_variant"
	for i := range DefaultAttributeCardinalityLimit {
		in := []attribute.KeyValue{attribute.String(customKey, fmt.Sprintf("ua-%d", i))}
		out := policy.SanitizeForSpan(in)
		require.Len(t, out, 1)
		assert.Equal(t, in[0], out[0], "attribute should be preserved before limit")
	}

	overLimit := []attribute.KeyValue{attribute.String(customKey, "ua-over-limit")}
	sanitized := policy.SanitizeForSpan(overLimit)
	require.Len(t, sanitized, 1)
	assert.Equal(t, attribute.String(customKey, AttributeHighCardinalityValue), sanitized[0])

	httpUserAgent := []attribute.KeyValue{attribute.String("http.user_agent", "ua-allowed")}
	httpUserAgentSanitized := policy.SanitizeForSpan(httpUserAgent)
	assert.Equal(t, httpUserAgent[0], httpUserAgentSanitized[0], "http.user_agent should be cardinality-exempt")
}

func TestResourceAttributesSet(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("resource-service"),
			semconv.ServiceNamespace("armada"),
			semconv.ServiceVersion("1.2.3"),
		),
	)
	require.NoError(t, err)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(NewSpanAttributePolicyProcessor(NewDefaultSpanAttributePolicy())),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	otel.SetTracerProvider(tp)

	tracer := tp.Tracer("resource-test")
	_, span := tracer.Start(context.Background(), "resource-span")
	span.SetAttributes(attribute.String("user_id", "pii-user"))
	span.End()

	spans := spanRecorder.Ended()
	require.Len(t, spans, 1)

	resourceAttrs := spans[0].Resource().Attributes()
	assert.Contains(t, resourceAttrs, semconv.ServiceName("resource-service"))
	assert.Contains(t, resourceAttrs, semconv.ServiceNamespace("armada"))
	assert.Contains(t, resourceAttrs, semconv.ServiceVersion("1.2.3"))

	// Policy processor must not mutate resource attributes.
	for _, kv := range resourceAttrs {
		assert.NotEqual(t, "user_id", string(kv.Key))
	}
}

func TestSpanAttributePolicySanitizesAttributesProvidedAtSpanStart(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(NewSpanAttributePolicyProcessor(NewDefaultSpanAttributePolicy())),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	tracer := tp.Tracer("start-attrs-test")
	_, span := tracer.Start(context.Background(), "start-attrs",
		oteltrace.WithAttributes(attribute.String("user_id", "pii-user-at-start")),
	)
	span.End()

	spans := spanRecorder.Ended()
	require.Len(t, spans, 1)
	endedAttrs := spans[0].Attributes()
	assert.Contains(t, endedAttrs, attribute.String("user_id", AttributeRedactedValue))

	violations := NewDefaultSpanAttributePolicy().ViolationsForSpan(endedAttrs)
	assert.NotContains(t, violations, SpanAttributeViolation{Key: "user_id", Reason: SpanAttributeViolationDenied})
}

func TestSpanAttributePolicyGuardrailDetectsDeniedAttributesSetAfterStart(t *testing.T) {
	policy := NewDefaultSpanAttributePolicy()
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(NewSpanAttributePolicyProcessor(policy)),
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	tracer := tp.Tracer("late-attrs-test")
	_, span := tracer.Start(context.Background(), "late-attrs")
	span.SetAttributes(attribute.String("user_id", "late-pii-user"))
	span.End()

	spans := spanRecorder.Ended()
	require.Len(t, spans, 1)

	endedAttrs := spans[0].Attributes()
	assert.Contains(t, endedAttrs, attribute.String("user_id", "late-pii-user"), "OTel processor cannot rewrite post-start attributes")

	violations := policy.ViolationsForSpan(endedAttrs)
	assert.Contains(t, violations, SpanAttributeViolation{Key: "user_id", Reason: SpanAttributeViolationDenied})
}
