package observability

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/armadaproject/armada/internal/lookout/version"
)

func TestServiceBootstrapPatternServerConfig(t *testing.T) {
	cfg, err := ReadObservabilityConfig(ResourceAttributes{
		ServiceName:     "server",
		ServiceVersion:  version.Version,
		ServiceInstance: uuid.New().String(),
	})
	require.NoError(t, err)
	require.Equal(t, "server", cfg.Resource.ServiceName)
	require.NotEmpty(t, cfg.Resource.ServiceInstance)
}

func TestServiceBootstrapPatternExecutorConfig(t *testing.T) {
	cfg, err := ReadObservabilityConfig(ResourceAttributes{
		ServiceName:     "executor",
		ServiceVersion:  version.Version,
		ServiceInstance: uuid.New().String(),
	})
	require.NoError(t, err)
	require.Equal(t, "executor", cfg.Resource.ServiceName)
}

func TestServiceBootstrapPatternSchedulerConfig(t *testing.T) {
	cfg, err := ReadObservabilityConfig(ResourceAttributes{
		ServiceName:     "scheduler",
		ServiceVersion:  version.Version,
		ServiceInstance: uuid.New().String(),
	})
	require.NoError(t, err)
	require.Equal(t, "scheduler", cfg.Resource.ServiceName)
}

func TestBootstrapWithDisabledOTel(t *testing.T) {
	cfg, err := ReadObservabilityConfig(ResourceAttributes{
		ServiceName:     "test-service",
		ServiceVersion:  "test",
		ServiceInstance: uuid.New().String(),
	})
	require.NoError(t, err)
	require.False(t, cfg.Enabled)

	err = InitOTel(cfg)
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
	defaults := ResourceAttributes{
		ServiceName:     "test-service",
		ServiceVersion:  "test",
		ServiceInstance: uuid.New().String(),
	}
	cfg, err := (ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: "http://localhost:19999",
		},
	}).WithDefaults(defaults)
	require.NoError(t, err)
	require.True(t, cfg.Enabled)

	err = InitOTel(cfg)
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
	cfg, err := (ObservabilityConfig{
		Resource: ResourceAttributes{
			ServiceName:    "configured-service",
			ServiceVersion: "configured-v1",
		},
	}).WithDefaults(ResourceAttributes{
		ServiceName:     "default-service",
		ServiceVersion:  "default-version",
		ServiceInstance: uuid.New().String(),
	})
	require.NoError(t, err)
	require.Equal(t, "configured-service", cfg.Resource.ServiceName)
	require.Equal(t, "configured-v1", cfg.Resource.ServiceVersion)
}

func TestBootstrapInitShutdownMultipleTimes(t *testing.T) {
	for i := 0; i < 3; i++ {
		defaults := ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "test",
			ServiceInstance: uuid.New().String(),
		}
		cfg, err := (ObservabilityConfig{
			Enabled: true,
			Exporter: OTLPExporterConfig{
				Endpoint: "http://localhost:19999",
			},
		}).WithDefaults(defaults)
		require.NoError(t, err)

		err = InitOTel(cfg)
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
