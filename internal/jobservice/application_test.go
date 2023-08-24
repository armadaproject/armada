package jobservice

import (
	"testing"

	"github.com/stretchr/testify/require"

	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
)

var knownGoodConfig = DefaultConfiguration

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
				SubscriberPoolSize: 0,
			},
			expectedConfig: knownGoodConfig,
		},
		{
			name: "Incorrect GrpcPool.InitialConnections",
			config: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{InitialConnections: 0, Capacity: knownGoodConfig.GrpcPool.Capacity},
			},
			expectedConfig: knownGoodConfig,
		},
		{
			name: "Incorrect GrpcPool.Capacity",
			config: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{InitialConnections: knownGoodConfig.GrpcPool.InitialConnections, Capacity: 0},
			},
			expectedConfig: knownGoodConfig,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			RectifyConfig(tc.config)
			require.Equal(t, tc.expectedConfig, tc.config)
		})
	}
}
