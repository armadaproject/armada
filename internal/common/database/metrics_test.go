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

// samplesByDatabase returns the value of the named gauge keyed by its "database" label ("" when unlabelled).
func samplesByDatabase(t *testing.T, registry *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			database := ""
			for _, label := range metric.GetLabel() {
				if label.GetName() == "database" {
					database = label.GetValue()
				}
			}
			out[database] = metric.GetGauge().GetValue()
		}
	}
	return out
}

func TestRegisterPoolMetrics(t *testing.T) {
	tests := map[string]struct {
		labels           []map[string]string
		expected         map[string]float64
		expectedErrorAt  int
		expectDuplicates bool
	}{
		"labelled": {
			labels:   []map[string]string{{"database": "main"}},
			expected: map[string]float64{"main": 7},
		},
		"two pools": {
			labels:   []map[string]string{{"database": "main"}, {"database": "mirror"}},
			expected: map[string]float64{"main": 7, "mirror": 7},
		},
		"nil labels": {
			labels:   []map[string]string{nil},
			expected: map[string]float64{"": 7},
		},
		"duplicate labels": {
			labels:           []map[string]string{{"database": "main"}, {"database": "mirror"}, {"database": "main"}},
			expected:         map[string]float64{"main": 7, "mirror": 7},
			expectedErrorAt:  2,
			expectDuplicates: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			for i, labels := range tc.labels {
				err := RegisterPoolMetrics(registry, newLazyPool(t), labels)
				if tc.expectDuplicates && i == tc.expectedErrorAt {
					assert.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			assert.Equal(t, tc.expected, samplesByDatabase(t, registry, "pgxpool_max_conns"))
		})
	}
}
