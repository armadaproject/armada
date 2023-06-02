package jobservice

import (
	"testing"

	"github.com/stretchr/testify/require"

	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
)

var knownGoodConfig = &configuration.JobServiceConfiguration{
	GrpcPool: grpcconfig.GrpcPoolConfig{
		InitialConnections: 10,
		Capacity:           10,
	},
	SubscriberPoolSize: 30,
}

func TestRectifyConfig(t *testing.T) {
	testCases := []struct {
		name           string
		config         *configuration.JobServiceConfiguration
		expectedConfig *configuration.JobServiceConfiguration
	}{
		{
			name:           "S'all good",
			config:         knownGoodConfig,
			expectedConfig: knownGoodConfig,
		},
		{
			name: "Zero-length SubscriberPoolSize",
			config: &configuration.JobServiceConfiguration{
				GrpcPool:           grpcconfig.GrpcPoolConfig{InitialConnections: 10, Capacity: 10},
				SubscriberPoolSize: 0,
			},
			expectedConfig: knownGoodConfig,
		},
		{
			name: "Incorrect GrpcPool.InitialConnections",
			config: &configuration.JobServiceConfiguration{
				GrpcPool:           grpcconfig.GrpcPoolConfig{InitialConnections: 0, Capacity: 10},
				SubscriberPoolSize: 30,
			},
			expectedConfig: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: DefaultConfiguration.GrpcPool.InitialConnections,
					Capacity:           10,
				},
				SubscriberPoolSize: 30,
			},
		},
		{
			name: "Incorrect GrpcPool.Capacity",
			config: &configuration.JobServiceConfiguration{
				GrpcPool:           grpcconfig.GrpcPoolConfig{InitialConnections: 10, Capacity: 0},
				SubscriberPoolSize: 30,
			},
			expectedConfig: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: 10,
					Capacity:           DefaultConfiguration.GrpcPool.Capacity,
				},
				SubscriberPoolSize: 30,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			RectifyConfig(tc.config)
			require.Equal(t, tc.expectedConfig, tc.config)
		})
	}
}
