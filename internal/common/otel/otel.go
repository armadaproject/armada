package otel

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/armadaproject/armada/internal/common/otel/configuration"
	"github.com/go-logr/stdr"
	"log"
	"os"

	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// otelProviders is a collection of misc OTEL providers to pass around
type otelProviders struct {
	tp         trace.TracerProvider
	mp         metric.MeterProvider
	propagator propagation.TextMapPropagator
}

func (p *otelProviders) shutdown(ctx context.Context) error {
	switch v := p.tp.(type) {
	case *sdktrace.TracerProvider:
		if err := v.Shutdown(ctx); err != nil {
			return err
		}
	}

	switch v := p.mp.(type) {
	case *sdkmetric.MeterProvider:
		if err := v.Shutdown(ctx); err != nil {
			return err
		}
	}

	return nil
}

func newNoopProviders() *otelProviders {
	return &otelProviders{tp: tracenoop.NewTracerProvider(), mp: metricnoop.NewMeterProvider(), propagator: propagation.NewCompositeTextMapPropagator()}
}

func getProviders(ctx context.Context, c *configuration.OtelConfig, r *resource.Resource) (*otelProviders, error) {
	var traceExp sdktrace.SpanExporter
	var metricExp sdkmetric.Exporter
	var err error

	switch c.ExportStrategy {
	case "stdout":
		traceExp, err = newStdOutTraceExporter()
		if err != nil {
			return nil, fmt.Errorf("creating stdout trace exporter: %w", err)
		}

		metricExp, err = newStdOutMetricExporter()
		if err != nil {
			return nil, fmt.Errorf("creating stdout metric exporter: %w", err)
		}
	case "http":
		traceExp, err = newHttpTraceExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating http trace exporter: %w", err)
		}

		metricExp, err = newHttpMetricExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating http metric exporter: %w", err)
		}
	case "grpc":
		traceExp, err = newGrpcTraceExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating grpc trace exporter: %w", err)
		}

		metricExp, err = newGrpcMetricExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating grpc metric exporter: %w", err)
		}
	default:
		providers := newNoopProviders()
		return providers, nil
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(r),
	)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(r),
	)

	// Unless no-op, always propagate trace context and baggage
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

	providers := &otelProviders{tp: tp, mp: mp, propagator: propagator}
	return providers, nil

}

// LoadOtel Loads the configured trace implementation and sets it as the OTEL tracer provider.
// Returns the closer which should be used to gracefully shutdown the tracer provider
// OTEL exporters can be further configured using OTEL environment variables.
func LoadOtel(ctx context.Context, c *configuration.OtelConfig, r *resource.Resource) (closer func(ctx context.Context) error, e error) {
	providers, err := getProviders(ctx, c, r)

	if err != nil {
		return nil, fmt.Errorf("getting otel otelProviders: %w", err)
	}

	// Set the otelProviders onto otel
	otel.SetTracerProvider(providers.tp)
	otel.SetMeterProvider(providers.mp)
	otel.SetTextMapPropagator(providers.propagator)
	otel.SetErrorHandler(NewDefaultErrorHandler())

	// For now use the go logger adapter. logr does not support slog yet: https://github.com/go-logr/logr/issues/171
	stdr.SetVerbosity(c.LogLevel)
	logger := stdr.New(log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile))
	otel.SetLogger(logger)

	return providers.shutdown, nil
}

// newStdOutTraceExporter returns a console exporter.
func newStdOutTraceExporter() (sdktrace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout),
		// Use human readable output.
		stdouttrace.WithPrettyPrint(),
		// Do not print timestamps for the demo.
		stdouttrace.WithoutTimestamps(),
	)
}

// newStdOutTraceExporter returns a console exporter.
func newStdOutMetricExporter() (sdkmetric.Exporter, error) {
	// Print with a JSON encoder that indents with two spaces.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	return stdoutmetric.New(
		stdoutmetric.WithEncoder(enc),
		stdoutmetric.WithoutTimestamps(),
	)
}

func newHttpTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP HTTP trace exporter: %w", err)
	}
	return exporter, nil
}

func newHttpMetricExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	exporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP HTTP metric exporter: %w", err)
	}

	return exporter, nil
}

func newGrpcTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	client := otlptracegrpc.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP gRPC trace exporter: %w", err)
	}
	return exporter, nil
}

func newGrpcMetricExporter(ctx context.Context) (sdkmetric.Exporter, error) {
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP gRPC metric exporter: %w", err)
	}

	return exporter, nil
}

// NewResource returns a resource describing this application.
func NewResource(serviceName string) (*resource.Resource, error) {

	attrs := []attribute.KeyValue{
		semconv.ServiceName(serviceName),
	}

	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	)

	if err != nil {
		return nil, fmt.Errorf("creating resource: %w", err)
	}
	return r, nil
}

type DefaultErrorHandler struct{}

func NewDefaultErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{}
}

func (DefaultErrorHandler) Handle(err error) {
	// FIXME: Update to use proper logger
	log.Printf("OTEL error: %v\n", err)
}
