package jobservice

import (
	"testing"

	grpcconfig "github.com/armadaproject/armada/internal/common/grpc/configuration"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/stretchr/testify/require"
)

var knownGoodConfig = &configuration.JobServiceConfiguration{
	GrpcPool: grpcconfig.GrpcPoolConfig{
		InitialConnections: 10,
		Capacity:           10,
	},
}

func TestRectifyConfig(t *testing.T) {
	testCases := []struct {
		name           string
		config         *configuration.JobServiceConfiguration
		expectedError  error
		expectedConfig *configuration.JobServiceConfiguration
	}{
		{
			name:           "S'all good",
			config:         knownGoodConfig,
			expectedError:  nil,
			expectedConfig: knownGoodConfig,
		},
		{
			name: "Incorrect GrpcPool.InitialConnections",
			config: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: 0,
					Capacity:           10,
				},
			},
			expectedError: nil,
			expectedConfig: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: DefaultConfiguration.GrpcPool.InitialConnections,
					Capacity:           10,
				},
			},
		},
		{
			name: "Incorrect GrpcPool.Capacity",
			config: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: 10,
					Capacity:           0,
				},
			},
			expectedError: nil,
			expectedConfig: &configuration.JobServiceConfiguration{
				GrpcPool: grpcconfig.GrpcPoolConfig{
					InitialConnections: 10,
					Capacity:           DefaultConfiguration.GrpcPool.Capacity,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := RectifyConfig(tc.config)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expectedConfig, tc.config)
		})
	}
}
