package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// InitTracing initializes OpenTelemetry with OTLP exporter
func InitTracing(serviceName string) (func(), error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4317" // Default SigNoz endpoint
	}

	// Create OTLP gRPC exporter
	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		trace.WithSampler(trace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down tracer: %v\n", err)
		}
	}, nil
}

// StartSpan starts a new span with the given component and operation name
func StartSpan(ctx context.Context, component, operation string) (context.Context, oteltrace.Span) {
	tracer := otel.Tracer(component)
	return tracer.Start(ctx, operation)
}

// AddErrorToSpan adds error information to a span
func AddErrorToSpan(span oteltrace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// AddSuccessToSpan marks a span as successful
func AddSuccessToSpan(span oteltrace.Span) {
	span.SetStatus(codes.Ok, "")
}

// DatabaseAttributes returns common attributes for database operations
func DatabaseAttributes(operation string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", operation),
	}
}

// JobAttributes returns common attributes for job operations
func JobAttributes(queue, jobset string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("armada.queue", queue),
		attribute.String("armada.jobset", jobset),
	}
}