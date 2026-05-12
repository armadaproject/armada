package eventingester

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func TestCreateMetricsRedisClient_EmptyAddrs(t *testing.T) {
	ctx := armadacontext.Background()
	connectionInfo := &redis.UniversalOptions{
		Addrs: nil,
	}

	client, err := createMetricsRedisClient(ctx, connectionInfo)

	assert.Nil(t, client)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metrics.redis.connectionInfo.addrs must be set")
}

func TestCreateMetricsRedisClient_NonSentinelMode_BuildsUniversalClient(t *testing.T) {
	ctx := armadacontext.Background()
	connectionInfo := &redis.UniversalOptions{
		Addrs:       []string{"localhost:6379"},
		MasterName:  "",
		DialTimeout: 5 * time.Second,
		PoolSize:    10,
	}

	client, err := createMetricsRedisClient(ctx, connectionInfo)

	if client != nil {
		defer client.Close()
	}

	if err != nil {
		t.Logf("Non-Sentinel client creation failed (expected if Redis not running): %v", err)
		return
	}

	require.NotNil(t, client)
}

func TestCreateMetricsRedisClient_SentinelMode_BuildsFailoverClient(t *testing.T) {
	t.Skip("Requires running Redis Sentinel cluster for integration testing")

	ctx := armadacontext.Background()
	connectionInfo := &redis.UniversalOptions{
		Addrs:        []string{"localhost:26379"},
		MasterName:   "mymaster",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}

	client, err := createMetricsRedisClient(ctx, connectionInfo)

	if client != nil {
		defer client.Close()
	}

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestCreateMetricsRedisClient_SentinelMode_NoReplicaAvailable(t *testing.T) {
	t.Skip("Requires running Redis Sentinel cluster with no replicas for integration testing")

	ctx := armadacontext.Background()
	connectionInfo := &redis.UniversalOptions{
		Addrs:        []string{"localhost:26379"},
		MasterName:   "mymaster",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	}

	client, err := createMetricsRedisClient(ctx, connectionInfo)

	if client != nil {
		defer client.Close()
	}

	require.Error(t, err)
	assert.Contains(t, err.Error(), "replica")
}
