package configuration

type OtelConfig struct {
	// Configured for which specific exporter to use.
	// Exporters can be further configured here: https://github.com/open-telemetry/opentelemetry-go/tree/main/exporters/otlp/otlptrace
	ExportStrategy string
	// This integer translates into the otel logging interface log level. 1=warn, 4=info, 8=debug
	LogLevel int
}
