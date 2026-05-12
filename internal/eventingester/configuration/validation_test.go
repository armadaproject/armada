package configuration

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func validBaseConfig() EventIngesterConfiguration {
	return EventIngesterConfiguration{
		Pulsar: commonconfig.PulsarConfig{
			URL: "pulsar://localhost:6650",
		},
		MetricsPort:      9000,
		SubscriptionName: "test-subscription",
		BatchSize:        100,
		BatchDuration:    1 * time.Second,
	}
}

func TestValidate_SentinelMode_ValidConfig(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      []string{"sentinel1:26379", "sentinel2:26379", "sentinel3:26379"},
				ReadOnly:   true,
			},
			CollectionInterval: 30 * time.Second,
			TopN:               100,
			ScanBatchSize:      1000,
			PipelineBatchSize:  100,
			MemoryUsageSamples: 5,
		},
	}

	err := config.Validate()
	assert.NoError(t, err, "Valid sentinel config should pass validation")
}

func TestValidate_SentinelMode_MissingAddrs(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      []string{},
				ReadOnly:   true,
			},
			CollectionInterval: 30 * time.Second,
		},
	}

	err := config.Validate()
	require.Error(t, err, "Sentinel mode with empty addrs should fail validation")
	assert.Contains(t, err.Error(), "metrics.redis.connectionInfo.addrs is required")
	assert.Contains(t, err.Error(), "sentinel mode is enabled")
}

func TestValidate_SentinelMode_MissingReadOnly(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      []string{"sentinel1:26379", "sentinel2:26379"},
				ReadOnly:   false,
			},
			CollectionInterval: 30 * time.Second,
		},
	}

	err := config.Validate()
	require.Error(t, err, "Sentinel mode with readOnly=false should fail validation")
	assert.Contains(t, err.Error(), "metrics.redis.connectionInfo.readOnly must be true")
	assert.Contains(t, err.Error(), "replica-only routing")
}

func TestValidate_SentinelMode_EmptyMasterName(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "",
				Addrs:      []string{"redis1:6379"},
				ReadOnly:   false,
			},
			CollectionInterval: 30 * time.Second,
		},
	}

	err := config.Validate()
	assert.NoError(t, err, "Non-sentinel mode should not require readOnly or specific addrs format")
}

func TestValidate_MetricsDisabled_NoValidation(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: false,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      []string{},
				ReadOnly:   false,
			},
		},
	}

	err := config.Validate()
	assert.NoError(t, err, "Sentinel validation should not apply when metrics are disabled")
}

func TestValidate_SentinelMode_NonStandardPort(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      []string{"sentinel1:26380", "sentinel2:26380"},
				ReadOnly:   true,
			},
			CollectionInterval: 30 * time.Second,
		},
	}

	err := config.Validate()
	assert.NoError(t, err, "Non-standard sentinel ports should be allowed")
}

func TestValidate_SentinelMode_NilAddrs(t *testing.T) {
	config := validBaseConfig()
	config.Metrics = MetricsConfig{
		Redis: RedisMemoryMetricsConfig{
			Enabled: true,
			ConnectionInfo: redis.UniversalOptions{
				MasterName: "mymaster",
				Addrs:      nil,
				ReadOnly:   true,
			},
			CollectionInterval: 30 * time.Second,
		},
	}

	err := config.Validate()
	require.Error(t, err, "Sentinel mode with nil addrs should fail validation")
	assert.Contains(t, err.Error(), "metrics.redis.connectionInfo.addrs is required")
}
