package database

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLazyPool returns a pool that never connects: pgxpool only dials when a connection is acquired.
func newLazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://user:pass@127.0.0.1:1/db?pool_max_conns=7")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestRegisterPoolMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "main"}))

	families, err := registry.Gather()
	require.NoError(t, err)

	var maxConns *float64
	for _, family := range families {
		if family.GetName() != "pgxpool_max_conns" {
			continue
		}
		require.Len(t, family.GetMetric(), 1)
		metric := family.GetMetric()[0]
		require.Len(t, metric.GetLabel(), 1)
		assert.Equal(t, "pool", metric.GetLabel()[0].GetName())
		assert.Equal(t, "main", metric.GetLabel()[0].GetValue())
		v := metric.GetGauge().GetValue()
		maxConns = &v
	}
	require.NotNil(t, maxConns, "pgxpool_max_conns not exported")
	assert.Equal(t, float64(7), *maxConns)
}

func TestRegisterPoolMetrics_TwoPoolsNeedDistinctLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "main"}))
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "mirror"}))
	assert.Error(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "main"}))
}

func samplesByPool(t *testing.T, registry *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			pool := ""
			for _, label := range metric.GetLabel() {
				if label.GetName() == "pool" {
					pool = label.GetValue()
				}
			}
			out[pool] = metric.GetGauge().GetValue()
		}
	}
	return out
}

func TestRegisterPoolMetrics_TwoPoolsAreBothScraped(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "main"}))
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), map[string]string{"pool": "mirror"}))
	assert.Equal(t, map[string]float64{"main": 7, "mirror": 7}, samplesByPool(t, registry, "pgxpool_max_conns"))
}

func TestRegisterPoolMetrics_NilLabelsGiveAnUnlabeledSample(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, RegisterPoolMetrics(registry, newLazyPool(t), nil))
	assert.Equal(t, map[string]float64{"": 7}, samplesByPool(t, registry, "pgxpool_max_conns"))
}
