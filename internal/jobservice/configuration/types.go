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
	// How long do we subscribe to a job set
	SubscribeJobSetTime int64
	// How often to do we write our in memory database to disk
	PersistenceInterval int64
	// TTL for our cache
	TimeToLiveCache int64
}
