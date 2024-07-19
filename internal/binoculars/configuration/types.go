package configuration

import (
	"github.com/armadaproject/armada/internal/common/auth/configuration"
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
)

type BinocularsConfig struct {
	Cordon CordonConfiguration
	Auth   configuration.AuthConfig

	GrpcPort    uint16
	HttpPort    uint16
	MetricsPort uint16
	Profiling   *profilingconfig.ProfilingConfig

	CorsAllowedOrigins []string

	Grpc             grpcconfig.GrpcConfig
	ImpersonateUsers bool
	Kubernetes       KubernetesConfiguration
}

type KubernetesConfiguration struct {
	Burst int
	QPS   float32
}

type CordonConfiguration struct {
	AdditionalLabels map[string]string
}
