package configuration

import (
	"github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
)

type BinocularsConfig struct {
	Auth configuration.AuthConfig

	GrpcPort           uint16
	HttpPort           uint16
	MetricsPort        uint16
	CorsAllowedOrigins []string

	Grpc             grpcconfig.GrpcConfig
	ImpersonateUsers bool
	Kubernetes       KubernetesConfiguration
}

type KubernetesConfiguration struct {
	Burst int
	QPS   float32
}
