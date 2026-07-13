package observability

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

const ServiceVersion = "0.1.0"

func TestServiceBootstrapPatternServerConfig(t *testing.T) {
	cfg := testBootstrapConfig("server", ServiceVersion, uuid.New().String())
	require.NoError(t, cfg.Validate())
	require.Equal(t, "server", cfg.Resource.ServiceName)
	require.NotEmpty(t, cfg.Resource.ServiceInstance)
}

func TestServiceBootstrapPatternExecutorConfig(t *testing.T) {
	cfg := testBootstrapConfig("executor", ServiceVersion, uuid.New().String())
	require.NoError(t, cfg.Validate())
	require.Equal(t, "executor", cfg.Resource.ServiceName)
}

func TestServiceBootstrapPatternSchedulerConfig(t *testing.T) {
	cfg := testBootstrapConfig("scheduler", ServiceVersion, uuid.New().String())
	require.NoError(t, cfg.Validate())
	require.Equal(t, "scheduler", cfg.Resource.ServiceName)
}

func TestBootstrapWithDisabledOTel(t *testing.T) {
	cfg := testBootstrapConfig("test-service", "test", uuid.New().String())
	require.False(t, cfg.Enabled)

	err := InitOTel(cfg)
	require.NoError(t, err)

	tp := otel.GetTracerProvider()
	tracer := tp.Tracer("test")
	_, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	spanCtx := span.SpanContext()
	require.False(t, spanCtx.IsValid(), "Span should not be valid when OTel is disabled")

	err = ShutdownOTel(context.Background())
	require.NoError(t, err)
}

func TestBootstrapWithEnabledOTelAndInvalidCollector(t *testing.T) {
	cfg := testBootstrapConfig("test-service", "test", uuid.New().String())
	cfg.Enabled = true
	cfg.Exporter.Endpoint = "http://localhost:19999"
	require.NoError(t, cfg.Validate())
	require.True(t, cfg.Enabled)

	err := InitOTel(cfg)
	require.NoError(t, err)

	tp := otel.GetTracerProvider()
	require.NotNil(t, tp)

	tracer := tp.Tracer("test-bootstrap")
	_, span := tracer.Start(context.Background(), "bootstrap-test-span")

	spanCtx := span.SpanContext()
	require.True(t, spanCtx.IsValid(), "Span should be valid even with unreachable collector (fail-open)")
	require.NotEmpty(t, spanCtx.TraceID().String())
	require.NotEmpty(t, spanCtx.SpanID().String())

	span.End()

	err = ShutdownOTel(context.Background())
	require.NoError(t, err)
}

func TestBootstrapResourceAttributesFromConfig(t *testing.T) {
	cfg := ObservabilityConfig{
		Exporter: OTLPExporterConfig{
			Endpoint: "http://otel-collector:4318",
			Protocol: testOTLPHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    testParentBasedTraceIDRatioSampler,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "configured-service",
			ServiceVersion:  "configured-v1",
			ServiceInstance: uuid.New().String(),
		},
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, "configured-service", cfg.Resource.ServiceName)
	require.Equal(t, "configured-v1", cfg.Resource.ServiceVersion)
}

func TestBootstrapInitShutdownMultipleTimes(t *testing.T) {
	for range 3 {
		cfg := ObservabilityConfig{
			Enabled: true,
			Exporter: OTLPExporterConfig{
				Endpoint: "http://localhost:19999",
				Protocol: testOTLPHTTPProtocol,
			},
			Traces: TracesConfig{
				Sampler:    testParentBasedTraceIDRatioSampler,
				SamplerArg: 1.0,
			},
			Resource: ResourceAttributes{
				ServiceName:     "test-service",
				ServiceVersion:  "test",
				ServiceInstance: uuid.New().String(),
			},
		}
		require.NoError(t, cfg.Validate())

		err := InitOTel(cfg)
		require.NoError(t, err)

		tp := otel.GetTracerProvider()
		tracer := tp.Tracer("test")
		_, span := tracer.Start(context.Background(), "test-span")
		require.True(t, span.SpanContext().IsValid())
		span.End()

		err = ShutdownOTel(context.Background())
		require.NoError(t, err)
	}
}

func testBootstrapConfig(serviceName, serviceServiceVersion, serviceInstance string) ObservabilityConfig {
	return ObservabilityConfig{
		Exporter: OTLPExporterConfig{
			Endpoint: "http://otel-collector:4318",
			Protocol: testOTLPHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    testParentBasedTraceIDRatioSampler,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     serviceName,
			ServiceVersion:  serviceServiceVersion,
			ServiceInstance: serviceInstance,
		},
	}
}
