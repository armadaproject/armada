package configuration

import "github.com/G-Research/armada/internal/common/auth/configuration"

type BinocularsConfig struct {
	Auth configuration.AuthConfig

	GrpcPort           uint16
	HttpPort           uint16
	MetricsPort        uint16
	CorsAllowedOrigins []string

	ImpersonateUsers bool
	Kubernetes       KubernetesConfiguration
}

type KubernetesConfiguration struct {
	Burst int
	QPS   float32
}
