package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservabilityConfig(t *testing.T) {
	defaults := ResourceAttributes{
		ServiceName:     "armada-server",
		ServiceVersion:  "1.2.3",
		ServiceInstance: "instance-1",
	}

	t.Run("uses defaults when config is unset", func(t *testing.T) {
		cfg, err := ReadObservabilityConfig(defaults)
		require.NoError(t, err)

		assert.False(t, cfg.Enabled)
		assert.Equal(t, DefaultOtlpHTTPEndpoint, cfg.Exporter.Endpoint)
		assert.Equal(t, DefaultOtlpHTTPProtocol, cfg.Exporter.Protocol)
		assert.Equal(t, SamplerParentBasedTraceRatio, cfg.Traces.Sampler)
		assert.Equal(t, 1.0, cfg.Traces.SamplerArg)
		assert.Equal(t, defaults, cfg.Resource)
	})

	t.Run("preserves config values", func(t *testing.T) {
		cfg, err := (ObservabilityConfig{
			Enabled: true,
			Exporter: OTLPExporterConfig{
				Endpoint: "http://otel-collector:4318",
				Protocol: "HTTP/PROTOBUF",
			},
			Traces: TracesConfig{
				Sampler:    "parent_based_trace_id_ratio",
				SamplerArg: 0.25,
			},
			Resource: ResourceAttributes{
				ServiceName:     "armada-executor",
				ServiceVersion:  "v2.0.0",
				ServiceInstance: "pod-xyz",
			},
		}).WithDefaults(defaults)
		require.NoError(t, err)

		assert.True(t, cfg.Enabled)
		assert.Equal(t, "http://otel-collector:4318", cfg.Exporter.Endpoint)
		assert.Equal(t, "http/protobuf", cfg.Exporter.Protocol)
		assert.Equal(t, SamplerParentBasedTraceRatio, cfg.Traces.Sampler)
		assert.Equal(t, 0.25, cfg.Traces.SamplerArg)
		assert.Equal(t, "armada-executor", cfg.Resource.ServiceName)
		assert.Equal(t, "v2.0.0", cfg.Resource.ServiceVersion)
		assert.Equal(t, "pod-xyz", cfg.Resource.ServiceInstance)
	})
}

func TestObservabilityConfigRejectsInvalidSampler(t *testing.T) {
	defaults := ResourceAttributes{
		ServiceName:     "armada-server",
		ServiceVersion:  "1.2.3",
		ServiceInstance: "instance-1",
	}

	_, err := (ObservabilityConfig{
		Traces: TracesConfig{Sampler: "invalid_sampler_name"},
	}).WithDefaults(defaults)
	require.Error(t, err)
	assert.ErrorContains(t, err, ConfigOtelTracesSampler)
	assert.ErrorContains(t, err, "invalid_sampler_name")
}

func TestObservabilityConfigRejectsUnsupportedEndpointScheme(t *testing.T) {
	defaults := ResourceAttributes{
		ServiceName:     "armada-server",
		ServiceVersion:  "1.2.3",
		ServiceInstance: "instance-1",
	}

	_, err := (ObservabilityConfig{
		Exporter: OTLPExporterConfig{
			Endpoint: "grpc://otel-collector:4317",
		},
	}).WithDefaults(defaults)
	require.Error(t, err)
	assert.ErrorContains(t, err, ConfigOtelExporterOtlpEndpoint)
	assert.ErrorContains(t, err, "http or https")
}
