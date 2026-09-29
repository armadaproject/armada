package scheduling

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRecordJobAggregateComparisonResult(t *testing.T) {
	pool := "metrics-test-pool"
	require.Equal(t, float64(0), testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))

	recordJobAggregateComparisonResult(pool, nil)
	recordJobAggregateComparisonResult(pool, []string{"demand_queued", "demand_queued", "demand_queued"})

	require.Equal(t, float64(2), testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
	require.Equal(t, float64(1), testutil.ToFloat64(jobAggregateMismatches.WithLabelValues(pool)))
	require.Equal(t, float64(3), testutil.ToFloat64(jobAggregateMismatchComponents.WithLabelValues(pool, "demand_queued")))
}
