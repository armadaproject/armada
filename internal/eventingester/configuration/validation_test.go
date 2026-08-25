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
		RetryMaxBackoff:     30 * time.Second,
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

func TestValidate_RejectsNegativeMaxBackoff(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryMaxBackoff = -1 * time.Second
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	err := config.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "retryMaxBackoff must be non-negative")
}

func TestValidate_RejectsInitialBackoffAboveMax(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryInitialBackoff = 40 * time.Second
	redisConfig.RetryMaxBackoff = 30 * time.Second
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	err := config.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "retryInitialBackoff (40s) must not exceed retryMaxBackoff (30s)")
}

func TestValidate_RejectsUnsetInitialBackoffWithMaxBelowDefault(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryInitialBackoff = 0
	redisConfig.RetryMaxBackoff = 100 * time.Millisecond
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	err := config.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "below the default retryInitialBackoff")
}

func TestValidate_AcceptsUnsetInitialBackoffWithMaxAtDefault(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryInitialBackoff = 0
	redisConfig.RetryMaxBackoff = DefaultRetryInitialBackoff
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	require.NoError(t, config.Validate())
}

func TestValidate_ReportsAllViolations(t *testing.T) {
	redisConfig := validRedisMemoryMetricsConfig()
	redisConfig.RetryInitialBackoff = -1 * time.Second
	redisConfig.RetryMaxBackoff = -2 * time.Second
	config := EventIngesterConfiguration{Metrics: MetricsConfig{Redis: redisConfig}}

	err := config.Validate()
	require.Error(t, err)
	require.ErrorContains(t, err, "retryInitialBackoff must be non-negative")
	require.ErrorContains(t, err, "retryMaxBackoff must be non-negative")
}
