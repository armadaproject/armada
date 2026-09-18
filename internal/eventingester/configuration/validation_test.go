package configuration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/leaderelection"
)

func validRedisMemoryMetricsConfig() RedisMemoryMetricsConfig {
	return RedisMemoryMetricsConfig{
		Enabled:             true,
		CollectionInterval:  time.Minute,
		TopN:                10,
		RetryInitialBackoff: 500 * time.Millisecond,
		Leader:              leaderelection.Config{Mode: leaderelection.ModeStandalone},
	}
}

func validEventIngesterConfiguration() EventIngesterConfiguration {
	return EventIngesterConfiguration{
		Metrics: MetricsConfig{
			Redis: validRedisMemoryMetricsConfig(),
		},
	}
}

func TestValidate_AcceptsValidRetryConfig(t *testing.T) {
	require.NoError(t, validEventIngesterConfiguration().Validate())
}

func TestValidate_AllowsZeroBackoffs(t *testing.T) {
	config := validEventIngesterConfiguration()
	config.Metrics.Redis = RedisMemoryMetricsConfig{
		Leader: leaderelection.Config{Mode: leaderelection.ModeStandalone},
	}
	require.NoError(t, config.Validate())
}

func TestValidate_RejectsNegativeInitialBackoff(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryInitialBackoff = -1 * time.Second
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	err := config.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "retryInitialBackoff must be non-negative")
}
