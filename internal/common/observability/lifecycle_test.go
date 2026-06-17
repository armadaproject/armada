package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestOtelLifecycleCollectorReachable(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	exportedSpans := make(chan []sdktrace.ReadOnlySpan, 1)

	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spans := spanRecorder.Ended()
		if len(spans) > 0 {
			exportedSpans <- spans
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-1",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err, "InitOTel should succeed with reachable collector")

	tp := otel.GetTracerProvider()
	assert.NotNil(t, tp, "Global tracer provider should be set")

	tracer := tp.Tracer("test-tracer")
	assert.NotNil(t, tracer, "Tracer should not be nil")

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	err = ShutdownOTel(context.Background())
	assert.NoError(t, err, "ShutdownOTel should succeed")
}

func TestOtelAPIsAreSafeBeforeInit(t *testing.T) {
	setNoopOTel()

	tracer := otel.Tracer("pre-init-tracer")
	meter := otel.Meter("pre-init-meter")
	require.NotNil(t, tracer)
	require.NotNil(t, meter)

	_, span := tracer.Start(context.Background(), "pre-init-span")
	require.NotPanics(t, func() { span.End() })

	_, err := meter.Int64Counter("pre_init_counter")
	require.NoError(t, err)
}

func TestOtelLifecycleFailOpenWhenCollectorDown(t *testing.T) {
	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: "http://localhost:19999",
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-2",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err, "InitOTel should succeed even when collector is unreachable (fail-open)")

	tp := otel.GetTracerProvider()
	assert.NotNil(t, tp, "Global tracer provider should be set even with unreachable collector")

	tracer := tp.Tracer("test-tracer")
	assert.NotNil(t, tracer, "Tracer should not be nil")

	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	err = ShutdownOTel(context.Background())
	assert.NoError(t, err, "ShutdownOTel should succeed")
}

func TestOtelShutdownFlushes(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    "always_on",
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-3",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err)

	tracer := otel.GetTracerProvider().Tracer("test-tracer")
	ctx := context.Background()
	_, span := tracer.Start(ctx, "test-span")
	span.End()

	start := time.Now()
	err = ShutdownOTel(context.Background())
	elapsed := time.Since(start)

	assert.NoError(t, err, "ShutdownOTel should succeed")
	assert.LessOrEqual(t, elapsed, shutdownTimeout+2*time.Second, "Shutdown should complete within timeout bounds")
}

func TestOtelDisabledWhenConfigDisabled(t *testing.T) {
	cfg := ObservabilityConfig{
		Enabled: false,
		Exporter: OTLPExporterConfig{
			Endpoint: "http://localhost:4318",
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-4",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err, "InitOTel should succeed when disabled")
	assert.IsType(t, tracenoop.TracerProvider{}, otel.GetTracerProvider())
	assert.IsType(t, metricnoop.MeterProvider{}, otel.GetMeterProvider())

	err = ShutdownOTel(context.Background())
	assert.NoError(t, err, "ShutdownOTel should succeed when OTel was disabled")
}

func TestOtelShutdownResetsToNoopProviders(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-noop-reset",
		},
	}

	require.NoError(t, InitOTel(cfg))
	require.NoError(t, ShutdownOTel(context.Background()))
	assert.IsType(t, tracenoop.TracerProvider{}, otel.GetTracerProvider())
	assert.IsType(t, metricnoop.MeterProvider{}, otel.GetMeterProvider())
}

func TestOtelPropagatorSetup(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-5",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err)

	propagator := otel.GetTextMapPropagator()
	assert.NotNil(t, propagator, "Global propagator should be set")

	carrier := make(map[string]string)
	ctx := context.Background()

	tracer := otel.GetTracerProvider().Tracer("test-tracer")
	ctx, span := tracer.Start(ctx, "test-span")
	defer span.End()

	propagator.Inject(ctx, &testCarrier{data: carrier})

	assert.Contains(t, carrier, "traceparent", "Propagator should inject W3C traceparent header")

	err = ShutdownOTel(context.Background())
	assert.NoError(t, err)
}

func TestOtelSamplerConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		sampler    string
		samplerArg float64
		shouldFail bool
	}{
		{
			name:       "parent_based_trace_id_ratio",
			sampler:    "parent_based_trace_id_ratio",
			samplerArg: 0.5,
			shouldFail: false,
		},
		{
			name:       "always_on",
			sampler:    "always_on",
			samplerArg: 1.0,
			shouldFail: false,
		},
		{
			name:       "always_off",
			sampler:    "always_off",
			samplerArg: 0.0,
			shouldFail: false,
		},
		{
			name:       "trace_id_ratio",
			sampler:    "trace_id_ratio",
			samplerArg: 0.1,
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer mockCollector.Close()

			cfg := ObservabilityConfig{
				Enabled: true,
				Exporter: OTLPExporterConfig{
					Endpoint: mockCollector.URL,
					Protocol: DefaultOtlpHTTPProtocol,
				},
				Traces: TracesConfig{
					Sampler:    tt.sampler,
					SamplerArg: tt.samplerArg,
				},
				Resource: ResourceAttributes{
					ServiceName:     "test-service",
					ServiceVersion:  "1.0.0",
					ServiceInstance: "test-instance-sampler",
				},
			}

			err := InitOTel(cfg)
			if tt.shouldFail {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if err == nil {
				tp := otel.GetTracerProvider()
				assert.NotNil(t, tp)
				_ = ShutdownOTel(context.Background())
			}
		})
	}
}

func TestSamplerDefaultsByConfig(t *testing.T) {
	defaults := ResourceAttributes{
		ServiceName:     "server",
		ServiceVersion:  "1.0.0",
		ServiceInstance: "test-defaults",
	}

	cfg, err := ReadObservabilityConfig(defaults)
	require.NoError(t, err)

	assert.Equal(t, SamplerParentBasedTraceRatio, cfg.Traces.Sampler)
	assert.Equal(t, 1.0, cfg.Traces.SamplerArg)
}

func TestSamplerOverrideByConfig(t *testing.T) {
	tests := []struct {
		name       string
		sampler    string
		samplerArg float64
		wantArg    float64
	}{
		{name: "always_on", sampler: "always_on", samplerArg: 0.5, wantArg: 0.5},
		{name: "always_off", sampler: "always_off", samplerArg: 0.5, wantArg: 0.5},
		{name: "trace_id_ratio", sampler: "trace_id_ratio", samplerArg: 0.10, wantArg: 0.10},
		{name: "parent_based_trace_id_ratio", sampler: SamplerParentBasedTraceRatio, samplerArg: 0.25, wantArg: 0.25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defaults := ResourceAttributes{
				ServiceName:     "server",
				ServiceVersion:  "1.0.0",
				ServiceInstance: "test-overrides",
			}

			cfg, err := (ObservabilityConfig{
				Traces: TracesConfig{
					Sampler:    tt.sampler,
					SamplerArg: tt.samplerArg,
				},
			}).WithDefaults(defaults)
			require.NoError(t, err)

			assert.Equal(t, tt.sampler, cfg.Traces.Sampler)
			assert.Equal(t, tt.wantArg, cfg.Traces.SamplerArg)
		})
	}
}

func TestSamplingRatioEnforcesValidRange(t *testing.T) {
	tests := []struct {
		name       string
		sampler    string
		samplerArg float64
		shouldFail bool
	}{
		{name: "trace_id_ratio below 0", sampler: "trace_id_ratio", samplerArg: -0.01, shouldFail: true},
		{name: "trace_id_ratio above 1", sampler: "trace_id_ratio", samplerArg: 1.01, shouldFail: true},
		{name: "parent_based_trace_id_ratio below 0", sampler: SamplerParentBasedTraceRatio, samplerArg: -0.5, shouldFail: true},
		{name: "parent_based_trace_id_ratio above 1", sampler: SamplerParentBasedTraceRatio, samplerArg: 2.0, shouldFail: true},
		{name: "trace_id_ratio at lower bound", sampler: "trace_id_ratio", samplerArg: 0.0, shouldFail: false},
		{name: "trace_id_ratio at upper bound", sampler: "trace_id_ratio", samplerArg: 1.0, shouldFail: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defaults := ResourceAttributes{
				ServiceName:     "server",
				ServiceVersion:  "1.0.0",
				ServiceInstance: "test-ratio-range",
			}

			cfg, err := (ObservabilityConfig{
				Traces: TracesConfig{
					Sampler:    tt.sampler,
					SamplerArg: tt.samplerArg,
				},
			}).WithDefaults(defaults)
			if tt.shouldFail {
				require.Error(t, err)
				assert.ErrorContains(t, err, ConfigOtelTracesSamplerArg)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sampler, cfg.Traces.Sampler)
			assert.Equal(t, tt.samplerArg, cfg.Traces.SamplerArg)
		})
	}
}

func TestExporterBackpressureSafetyBounds(t *testing.T) {
	assert.Equal(t, 512, maxExportBatch)
	assert.Equal(t, 2048, maxBatchQueue)
	assert.Equal(t, 10*time.Second, exportTimeout)
	assert.Equal(t, 5*time.Second, batchTimeout)

	// Slow collector to induce exporter pressure without failing InitOTel.
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    "always_on",
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-backpressure",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ShutdownOTel(context.Background())
	})

	tracer := otel.GetTracerProvider().Tracer("backpressure-test")
	start := time.Now()
	for range maxBatchQueue * 2 {
		_, span := tracer.Start(context.Background(), "backpressure-span")
		span.End()
	}
	elapsed := time.Since(start)

	// If queue is bounded and drop-on-backpressure is active, span creation/end should remain fast.
	assert.Less(t, elapsed, 3*time.Second)
}

type testCarrier struct {
	data map[string]string
}

func (c *testCarrier) Get(key string) string {
	return c.data[key]
}

func (c *testCarrier) Set(key, value string) {
	c.data[key] = value
}

func (c *testCarrier) Keys() []string {
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

func TestOtelMultipleInitShutdownCycles(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-multi",
		},
	}

	for i := range 3 {
		err := InitOTel(cfg)
		require.NoError(t, err, "InitOTel cycle %d should succeed", i)

		tracer := otel.GetTracerProvider().Tracer("test-tracer")
		ctx := context.Background()
		_, span := tracer.Start(ctx, "test-span")
		span.End()

		err = ShutdownOTel(context.Background())
		assert.NoError(t, err, "ShutdownOTel cycle %d should succeed", i)
	}
}

func TestOtelInitWithInvalidResourceAttributes(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-invalid",
		},
	}

	err := cfg.Validate()
	assert.Error(t, err, "Config validation should fail with empty service name")
}

func TestOtelShutdownWithoutInit(t *testing.T) {
	setNoopOTel()
	err := ShutdownOTel(context.Background())
	assert.NoError(t, err, "ShutdownOTel should handle nil provider gracefully")
}

func TestOtelTracerProviderIsGloballySet(t *testing.T) {
	mockCollector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockCollector.Close()

	cfg := ObservabilityConfig{
		Enabled: true,
		Exporter: OTLPExporterConfig{
			Endpoint: mockCollector.URL,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "test-service",
			ServiceVersion:  "1.0.0",
			ServiceInstance: "test-instance-global",
		},
	}

	err := InitOTel(cfg)
	require.NoError(t, err)

	tp := otel.GetTracerProvider()
	_, ok := tp.(*sdktrace.TracerProvider)
	assert.True(t, ok, "Global tracer provider should be of type *sdktrace.TracerProvider")

	tracer := tp.Tracer("integration-test")
	ctx := context.Background()
	_, span := tracer.Start(ctx, "global-span-test")

	spanContext := span.SpanContext()
	assert.True(t, spanContext.IsValid(), "Span context should be valid")
	assert.True(t, spanContext.TraceID().IsValid(), "Trace ID should be valid")
	assert.True(t, spanContext.SpanID().IsValid(), "Span ID should be valid")

	span.End()

	err = ShutdownOTel(context.Background())
	assert.NoError(t, err)
}
