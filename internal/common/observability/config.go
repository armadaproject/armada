package observability

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

const (
	DefaultOtlpHTTPEndpoint      = "http://localhost:4318"
	DefaultOtlpHTTPProtocol      = "http/protobuf"
	SamplerParentBasedTraceRatio = "parent_based_trace_id_ratio"
)

const (
	ConfigOtelExporterOtlpEndpoint = "observability.exporter.endpoint"
	ConfigOtelExporterOtlpProtocol = "observability.exporter.protocol"
	ConfigOtelTracesSampler        = "observability.traces.sampler"
	ConfigOtelTracesSamplerArg     = "observability.traces.samplerArg"
)

const (
	ResourceAttributeServiceName     = "service.name"
	ResourceAttributeServiceVersion  = "service.version"
	ResourceAttributeServiceInstance = "service.instance.id"
)

var validSamplers = map[string]bool{
	SamplerParentBasedTraceRatio: true,
	"trace_id_ratio":             true,
	"always_on":                  true,
	"always_off":                 true,
}

var validOTLPProtocols = map[string]struct{}{
	"http/protobuf": {},
	"grpc":          {},
}

// ResourceAttributes define the required OpenTelemetry service identity contract.
type ResourceAttributes struct {
	// Service name is the name of the service.
	// Required attribute, with key `service.name`.
	ServiceName string
	// Service version is the version of the service.
	// Required attribute, with key `service.version`.
	ServiceVersion string
	// Service instance is a unique identifier for the service instance.
	// Required attribute, with key `service.instance.id`.
	ServiceInstance string
	// Extra attributes if specified will be added to the resource attributes.
	// These can be used to add additional metadata about the service instance.
	Extra map[string]string
}

type OTLPExporterConfig struct {
	Endpoint string
	Protocol string
}

type TracesConfig struct {
	// Sampler controls root trace sampling policy.
	// Supported values:
	//   - always_on
	//   - always_off
	//   - trace_id_ratio
	//   - parent_based_trace_id_ratio
	//
	// Default is parent_based_trace_id_ratio to respect upstream sampling decisions
	// while still allowing root-span ratio sampling in this service.
	Sampler string
	// SamplerArg is the sampler parameter used by ratio samplers.
	// Valid range for ratio samplers is 0.0 to 1.0.
	SamplerArg float64
}

type ObservabilityConfig struct {
	Enabled  bool
	Exporter OTLPExporterConfig
	Traces   TracesConfig
	Resource ResourceAttributes
}

func DefaultObservabilityConfig(defaultResource ResourceAttributes) ObservabilityConfig {
	return ObservabilityConfig{
		Enabled: false,
		Exporter: OTLPExporterConfig{
			Endpoint: DefaultOtlpHTTPEndpoint,
			Protocol: DefaultOtlpHTTPProtocol,
		},
		Traces: TracesConfig{
			Sampler:    SamplerParentBasedTraceRatio,
			SamplerArg: 1.0,
		},
		Resource: defaultResource,
	}
}

func ReadObservabilityConfig(defaultResource ResourceAttributes) (ObservabilityConfig, error) {
	return ObservabilityConfig{}.WithDefaults(defaultResource)
}

func (c ObservabilityConfig) WithDefaults(defaultResource ResourceAttributes) (ObservabilityConfig, error) {
	defaults := DefaultObservabilityConfig(defaultResource)

	if strings.TrimSpace(c.Exporter.Endpoint) == "" {
		c.Exporter.Endpoint = defaults.Exporter.Endpoint
	} else {
		c.Exporter.Endpoint = strings.TrimSpace(c.Exporter.Endpoint)
	}

	if strings.TrimSpace(c.Exporter.Protocol) == "" {
		c.Exporter.Protocol = defaults.Exporter.Protocol
	} else {
		c.Exporter.Protocol = strings.ToLower(strings.TrimSpace(c.Exporter.Protocol))
	}

	if strings.TrimSpace(c.Traces.Sampler) == "" {
		c.Traces.Sampler = defaults.Traces.Sampler
		c.Traces.SamplerArg = defaults.Traces.SamplerArg
	} else {
		c.Traces.Sampler = strings.ToLower(strings.TrimSpace(c.Traces.Sampler))
	}

	if strings.TrimSpace(c.Resource.ServiceName) == "" {
		c.Resource.ServiceName = defaults.Resource.ServiceName
	}
	if strings.TrimSpace(c.Resource.ServiceVersion) == "" {
		c.Resource.ServiceVersion = defaults.Resource.ServiceVersion
	}
	if strings.TrimSpace(c.Resource.ServiceInstance) == "" {
		c.Resource.ServiceInstance = defaults.Resource.ServiceInstance
	}

	if err := c.Validate(); err != nil {
		return ObservabilityConfig{}, err
	}

	return c, nil
}

func (c ObservabilityConfig) Validate() error {
	if strings.TrimSpace(c.Exporter.Endpoint) == "" {
		return fmt.Errorf("%s must not be empty", ConfigOtelExporterOtlpEndpoint)
	}

	parsedURL, err := url.Parse(c.Exporter.Endpoint)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fmt.Errorf("%s must be a valid absolute URL", ConfigOtelExporterOtlpEndpoint)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("%s scheme must be http or https", ConfigOtelExporterOtlpEndpoint)
	}

	if _, ok := validOTLPProtocols[c.Exporter.Protocol]; !ok {
		return fmt.Errorf("%s=%q is invalid: supported values are %v", ConfigOtelExporterOtlpProtocol, c.Exporter.Protocol, sortedKeys(validOTLPProtocols))
	}

	if _, ok := validSamplers[c.Traces.Sampler]; !ok {
		return fmt.Errorf("%s=%q is invalid: supported values are %v", ConfigOtelTracesSampler, c.Traces.Sampler, sortedKeys(validSamplers))
	}

	if c.Traces.Sampler == SamplerParentBasedTraceRatio || c.Traces.Sampler == "parentbased_traceidratio" || c.Traces.Sampler == "trace_id_ratio" || c.Traces.Sampler == "traceidratio" {
		if c.Traces.SamplerArg < 0 || c.Traces.SamplerArg > 1 {
			return fmt.Errorf("%s must be between 0 and 1 for sampler %q", ConfigOtelTracesSamplerArg, c.Traces.Sampler)
		}
	}

	if strings.TrimSpace(c.Resource.ServiceName) == "" {
		return fmt.Errorf("resource attribute %q must not be empty", ResourceAttributeServiceName)
	}
	if strings.TrimSpace(c.Resource.ServiceVersion) == "" {
		return fmt.Errorf("resource attribute %q must not be empty", ResourceAttributeServiceVersion)
	}
	if strings.TrimSpace(c.Resource.ServiceInstance) == "" {
		return fmt.Errorf("resource attribute %q must not be empty", ResourceAttributeServiceInstance)
	}

	return nil
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
