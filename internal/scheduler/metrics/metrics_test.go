package metrics

import (
	"github.com/armadaproject/armada/internal/scheduler/schedulerresult"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReportSchedulerResult(t *testing.T) {
	tests := map[string]struct {
		schedulerResult schedulerresult.SchedulerResult
	}{
		"JobsConsidered": {},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			metrics, err := New(nil, nil)
			require.NoError(t, err)
			metrics.ReportSchedulerResult(x)
		})
	}
}
