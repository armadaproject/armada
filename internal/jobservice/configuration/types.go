package configuration

import (
	grpcconfig "github.com/G-Research/armada/internal/common/grpc/configuration"
	"github.com/G-Research/armada/pkg/client"

)

type JobServiceConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig
	ApiConnection client.ApiConnectionDetails

}
