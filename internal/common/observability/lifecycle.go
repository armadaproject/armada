package observability

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/armadaproject/armada/internal/common/logging"
)

const (
	exportTimeout   = 10 * time.Second
	batchTimeout    = 5 * time.Second
	maxExportBatch  = 512
	maxBatchQueue   = 2048
	shutdownTimeout = 5 * time.Second
)

var (
	globalTracerProvider   *sdktrace.TracerProvider
	globalTracerProviderMu sync.RWMutex
)

func setNoopOTelLocked() {
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	globalTracerProvider = nil
}

func setNoopOTel() {
	globalTracerProviderMu.Lock()
	tp := globalTracerProvider
	setNoopOTelLocked()
	globalTracerProviderMu.Unlock()

	if err := shutdownTracerProvider(context.Background(), tp); err != nil {
		logging.WithError(err).Warn("Failed to shutdown previous OTel tracer provider")
	}
}

func init() {
	setNoopOTel()
}

// InitOTel initializes the global OpenTelemetry tracer provider with the given configuration.
// This function is fail-open: if the OTLP collector is unreachable, it logs the error but
// returns success to allow the service to start. The tracer provider is set globally via
// otel.SetTracerProvider() and W3C propagators are registered via otel.SetTextMapPropagator().
//
// Returns an error only if configuration is invalid or critical setup fails (not collector reachability).
func InitOTel(cfg ObservabilityConfig) error {
	if !cfg.Enabled {
		setNoopOTel()
		logging.Info("OpenTelemetry disabled by config")
		return nil
	}

	attrs := []attribute.KeyValue{
		attribute.String(ResourceAttributeServiceName, cfg.Resource.ServiceName),
		attribute.String(ResourceAttributeServiceVersion, cfg.Resource.ServiceVersion),
		attribute.String(ResourceAttributeServiceInstance, cfg.Resource.ServiceInstance),
	}
	for key, value := range cfg.Resource.Extra {
		attrs = append(attrs, attribute.String(key, value))
	}
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attrs...,
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create OTel resource: %w", err)
	}

	// Create OTLP exporter with bounded timeout
	ctx, cancel := context.WithTimeout(context.Background(), exportTimeout)
	defer cancel()

	exporter, err := newTraceExporter(ctx, cfg)
	if err != nil {
		// Fail-open: log error but continue with noop provider
		logging.WithError(err).Warnf(
			"Failed to create OTLP trace exporter (endpoint=%s, protocol=%s). Service will start without tracing.",
			cfg.Exporter.Endpoint,
			cfg.Exporter.Protocol,
		)
		setNoopOTel()
		return nil
	}

	// Create sampler based on config
	var sampler sdktrace.Sampler
	switch cfg.Traces.Sampler {
	case "parent_based_trace_id_ratio":
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Traces.SamplerArg))
	case "trace_id_ratio":
		sampler = sdktrace.TraceIDRatioBased(cfg.Traces.SamplerArg)
	case "always_on":
		sampler = sdktrace.AlwaysSample()
	case "always_off":
		sampler = sdktrace.NeverSample()
	default:
		return fmt.Errorf("unsupported sampler: %s", cfg.Traces.Sampler)
	}

	// Create tracer provider with bounded batch processing guardrails:
	//   - max export batch size: 512 spans
	//   - max queue size: 2048 spans (drop-on-backpressure above this bound)
	//   - batch timeout: 5s
	//   - export timeout: 10s
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(NewSpanAttributePolicyProcessor(NewDefaultSpanAttributePolicy())),
		sdktrace.WithBatcher(
			exporter,
			sdktrace.WithMaxExportBatchSize(maxExportBatch),
			sdktrace.WithMaxQueueSize(maxBatchQueue),
			sdktrace.WithExportTimeout(exportTimeout),
			sdktrace.WithBatchTimeout(batchTimeout),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global tracer provider and propagators.
	globalTracerProviderMu.Lock()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	globalTracerProvider = tp
	globalTracerProviderMu.Unlock()

	logging.Infof(
		"OpenTelemetry initialized: endpoint=%s, sampler=%s, ratio=%.2f, service=%s",
		cfg.Exporter.Endpoint,
		cfg.Traces.Sampler,
		cfg.Traces.SamplerArg,
		cfg.Resource.ServiceName,
	)

	return nil
}

// ShutdownWithDefaultTimeout gracefully shuts down the global tracer provider with a default timeout.
// The default timeout is 5s.
func ShutdownWithDefaultTimeout() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return ShutdownOTel(ctx)
}

// ShutdownOTel gracefully shuts down the global tracer provider, flushing any
// pending spans to the collector.
func ShutdownOTel(ctx context.Context) error {
	globalTracerProviderMu.Lock()
	tp := globalTracerProvider
	setNoopOTelLocked()
	globalTracerProviderMu.Unlock()

	return shutdownTracerProvider(ctx, tp)
}

func shutdownTracerProvider(ctx context.Context, tp *sdktrace.TracerProvider) error {
	if tp == nil {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if err := tp.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("failed to shutdown OTel tracer provider: %w", err)
	}

	logging.Info("OpenTelemetry tracer provider shut down successfully")
	return nil
}

func newTraceExporter(ctx context.Context, cfg ObservabilityConfig) (sdktrace.SpanExporter, error) {
	parsedURL, err := url.Parse(cfg.Exporter.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse exporter endpoint: %w", err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil, fmt.Errorf("unsupported endpoint scheme %q: must be http or https", parsedURL.Scheme)
	}

	switch cfg.Exporter.Protocol {
	case "http/protobuf":
		exporterOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpointURL(cfg.Exporter.Endpoint),
			otlptracehttp.WithTimeout(exportTimeout),
		}
		return otlptracehttp.New(ctx, exporterOpts...)
	case "grpc":
		exporterOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpointURL(cfg.Exporter.Endpoint),
			otlptracegrpc.WithTimeout(exportTimeout),
		}
		return otlptracegrpc.New(ctx, exporterOpts...)
	default:
		return nil, fmt.Errorf("unsupported OTLP protocol: %s", cfg.Exporter.Protocol)
	}
}
