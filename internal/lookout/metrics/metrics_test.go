package metrics

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookout/schema"
)

func TestJobTablePartitionedGauge(t *testing.T) {
	tests := map[string]struct {
		partition bool
		expected  float64
	}{
		"unpartitioned": {partition: false, expected: 0},
		"partitioned":   {partition: true, expected: 1},
	}
	migrations, err := schema.LookoutMigrations()
	require.NoError(t, err)
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
				gauge := newJobTablePartitionedGauge(db)
				if tc.partition {
					require.Equal(t, 0.0, testutil.ToFloat64(gauge))
					require.NoError(t, schema.PartitionJobTable(armadacontext.Background(), db))
				}

				assert.Equal(t, tc.expected, testutil.ToFloat64(gauge))
				return nil
			})
			require.NoError(t, err)
		})
	}
}
