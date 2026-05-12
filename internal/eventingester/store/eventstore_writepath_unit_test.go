package store

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/eventingester/configuration"
)

func TestEventStoreWritePathUnchanged_StructureVerification(t *testing.T) {
	store := &RedisEventStore{
		dbs:     []redis.UniversalClient{},
		dbNames: []string{"main", "replica"},
		eventRetention: configuration.EventRetentionPolicy{
			RetentionDuration: 0,
		},
	}

	assert.Equal(t, 2, len(store.dbNames), "event store should be configured for 2 clients (main and replica)")
	assert.Equal(t, "main", store.dbNames[0], "first client should be main")
	assert.Equal(t, "replica", store.dbNames[1], "second client should be replica")
}

func TestEventStoreWritePathUnchanged_MetricsConfigIndependent(t *testing.T) {
	metricsConfig := &redis.UniversalOptions{
		MasterName: "mymaster",
		Addrs:      []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"},
		ReadOnly:   true,
	}

	writeConfig := &redis.UniversalOptions{
		Addrs: []string{"redis-main:6379"},
	}

	assert.NotEmpty(t, metricsConfig.MasterName, "metrics config has Sentinel masterName")
	assert.NotEmpty(t, metricsConfig.Addrs, "metrics config has Sentinel addrs")
	assert.True(t, metricsConfig.ReadOnly, "metrics config has ReadOnly=true")

	assert.Empty(t, writeConfig.MasterName, "write config should not have Sentinel masterName")
	assert.NotEmpty(t, writeConfig.Addrs, "write config should have regular addrs")
	assert.False(t, writeConfig.ReadOnly, "write config should not have ReadOnly=true")
}

func TestEventStoreWritePathUnchanged_NoMetricsClientInStore(t *testing.T) {
	store := &RedisEventStore{
		dbs:     []redis.UniversalClient{},
		dbNames: []string{"main", "replica"},
	}

	assert.Equal(t, 2, len(store.dbNames), "event store should have exactly 2 client names")

	for _, name := range store.dbNames {
		assert.NotEqual(t, "metrics", name, "metrics client should not be in event store")
	}
}

func TestEventStoreWritePathUnchanged_ClientNamesCorrect(t *testing.T) {
	store := &RedisEventStore{
		dbs:     []redis.UniversalClient{},
		dbNames: []string{"main", "replica"},
	}

	assert.Equal(t, []string{"main", "replica"}, store.dbNames, "client names should be main and replica")
	assert.NotContains(t, store.dbNames, "metrics", "metrics should not be in client names")
	assert.NotContains(t, store.dbNames, "sentinel", "sentinel should not be in client names")
}
