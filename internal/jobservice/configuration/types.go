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
	// Connection details that we obtain from client
	ApiConnection client.ApiConnectionDetails
	// Configurable value that translates to number of seconds
	// This is the amount of time since the last job in job-set has been updated.
	SubscribeJobSetTime int64
	// How often to do we write our in memory database to disk
	PersistenceInterval int64
}
