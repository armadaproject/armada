package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testOTLPHTTPProtocol               = "http/protobuf"
	testParentBasedTraceIDRatioSampler = "parent_based_trace_id_ratio"
)

func TestObservabilityConfig(t *testing.T) {
	t.Run("rejects unset config", func(t *testing.T) {
		err := (ObservabilityConfig{}).Validate()
		require.Error(t, err)

		assert.ErrorContains(t, err, ConfigOtelExporterOtlpEndpoint)
	})

	t.Run("preserves config values", func(t *testing.T) {
		cfg := ObservabilityConfig{
			Enabled: true,
			Exporter: OTLPExporterConfig{
				Endpoint: "http://otel-collector:4318",
				Protocol: testOTLPHTTPProtocol,
			},
			Traces: TracesConfig{
				Sampler:    testParentBasedTraceIDRatioSampler,
				SamplerArg: 0.25,
			},
			Resource: ResourceAttributes{
				ServiceName:     "armada-executor",
				ServiceVersion:  "v2.0.0",
				ServiceInstance: "pod-xyz",
			},
		}
		require.NoError(t, cfg.Validate())

		assert.True(t, cfg.Enabled)
		assert.Equal(t, "http://otel-collector:4318", cfg.Exporter.Endpoint)
		assert.Equal(t, "http/protobuf", cfg.Exporter.Protocol)
		assert.Equal(t, testParentBasedTraceIDRatioSampler, cfg.Traces.Sampler)
		assert.Equal(t, 0.25, cfg.Traces.SamplerArg)
		assert.Equal(t, "armada-executor", cfg.Resource.ServiceName)
		assert.Equal(t, "v2.0.0", cfg.Resource.ServiceVersion)
		assert.Equal(t, "pod-xyz", cfg.Resource.ServiceInstance)
	})

	t.Run("preserves explicit extra resource attributes", func(t *testing.T) {
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
				ServiceName:     "armada-executor",
				ServiceVersion:  "v2.0.0",
				ServiceInstance: "pod-xyz",
				Extra: map[string]string{
					"deployment.environment": "prod",
				},
			},
		}
		require.NoError(t, cfg.Validate())

		assert.Equal(t, map[string]string{"deployment.environment": "prod"}, cfg.Resource.Extra)
	})
}

func TestObservabilityConfigRejectsInvalidSampler(t *testing.T) {
	err := (ObservabilityConfig{
		Exporter: OTLPExporterConfig{
			Endpoint: "http://otel-collector:4318",
			Protocol: testOTLPHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    "invalid_sampler_name",
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "armada-server",
			ServiceVersion:  "1.2.3",
			ServiceInstance: "instance-1",
		},
	}).Validate()
	require.Error(t, err)
	assert.ErrorContains(t, err, ConfigOtelTracesSampler)
	assert.ErrorContains(t, err, "invalid_sampler_name")
}

func TestObservabilityConfigRejectsUnsupportedEndpointScheme(t *testing.T) {
	err := (ObservabilityConfig{
		Exporter: OTLPExporterConfig{
			Endpoint: "grpc://otel-collector:4317",
			Protocol: testOTLPHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    testParentBasedTraceIDRatioSampler,
			SamplerArg: 1.0,
		},
		Resource: ResourceAttributes{
			ServiceName:     "armada-server",
			ServiceVersion:  "1.2.3",
			ServiceInstance: "instance-1",
		},
	}).Validate()
	require.Error(t, err)
	assert.ErrorContains(t, err, ConfigOtelExporterOtlpEndpoint)
	assert.ErrorContains(t, err, "http or https")
}
