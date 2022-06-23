package configuration

import (
	grpcconfig "github.com/G-Research/armada/internal/common/grpc/configuration"
)

type JobCacheConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig
}
