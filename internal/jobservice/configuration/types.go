package configuration

import (
	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/pkg/client"
)

type JobServiceConfiguration struct {
	HttpPort    uint16
	GrpcPort    uint16
	MetricsPort uint16

	Grpc grpcconfig.GrpcConfig
	// Connection details that we obtain from client
	ApiConnection client.ApiConnectionDetails
	// Configurable value that translates to number of seconds
	// This is the amount of time since the last job in job-set has been updated.
	SubscribeJobSetTime int64
	// Absolute or relative path for sqllite database and must include the db name
	DatabasePath string
}
