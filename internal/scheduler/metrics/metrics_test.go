package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReportScheduleCycleTime(t *testing.T) {
	metrics, err := New(nil, nil)
	require.NoError(t, err)

	// Observe some values
	metrics.ReportScheduleCycleTime(10)
	metrics.ReportScheduleCycleTime(11.0)
	metrics.ReportScheduleCycleTime(12.0)

	sum := testutil.ToFloat64(scheduleCycleTimeMetric.(prometheus.Collector))
	count := testutil.CollectAndCount(scheduleCycleTimeMetric)

	assert.Equal(t, 33.0, sum)
	assert.Equal(t, 3, count)

}

func TestReportReconcileCycleTime(t *testing.T) {
	metrics, err := New(nil, nil)
	require.NoError(t, err)

	// Observe some values
	metrics.ReportReconcileCycleTime(10)
	metrics.ReportReconcileCycleTime(11.0)
	metrics.ReportReconcileCycleTime(12.0)

	sum := testutil.ToFloat64(reconciliationCycleTimeMetric.(prometheus.Collector))
	count := testutil.CollectAndCount(reconciliationCycleTimeMetric)

	assert.Equal(t, 33.0, sum)
	assert.Equal(t, 3, count)

}
