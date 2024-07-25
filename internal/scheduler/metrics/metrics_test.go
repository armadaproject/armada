package metrics

import (
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReportCycleTimes(t *testing.T) {
	tests := map[string]struct {
		scheduleCycleTimes             []time.Duration
		reconcileCycleTimes            []time.Duration
		expectedScheduleCycleTimesSum  float64
		expectedReconciliationTimesSum float64
	}{
		//"No entries": {},
		//"One entry": {
		//	scheduleCycleTimes:  []time.Duration{1 * time.Second},
		//	reconcileCycleTimes: []time.Duration{2 * time.Second},
		//},
		"two entries": {
			scheduleCycleTimes:  []time.Duration{1 * time.Second, 1 * time.Millisecond},
			reconcileCycleTimes: []time.Duration{2 * time.Second, 2 * time.Millisecond},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			metrics, err := New(nil, nil)
			require.NoError(t, err)

			for _, x := range tc.scheduleCycleTimes {
				metrics.ReportScheduleCycleTime(x)
			}
			for _, x := range tc.reconcileCycleTimes {
				metrics.ReportReconcileCycleTime(x)
			}
			testutil.ToFloat64(scheduleCycleTimeMetric)
		})
	}
}
