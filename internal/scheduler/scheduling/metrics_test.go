package scheduling

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRecordJobAggregateCanaryResult(t *testing.T) {
	pool := "metrics-test-pool"
	require.Equal(t, float64(0), testutil.ToFloat64(jobAggregateCanaryComparisons.WithLabelValues(pool)))

	recordJobAggregateCanaryResult(pool, nil)
	recordJobAggregateCanaryResult(pool, []string{"demand_queued", "demand_queued", "demand_queued"})

	require.Equal(t, float64(2), testutil.ToFloat64(jobAggregateCanaryComparisons.WithLabelValues(pool)))
	require.Equal(t, float64(1), testutil.ToFloat64(jobAggregateCanaryMismatches.WithLabelValues(pool)))
	require.Equal(t, float64(3), testutil.ToFloat64(jobAggregateCanaryMismatchComponents.WithLabelValues(pool, "demand_queued")))
}
