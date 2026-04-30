package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRecordStateUpdates(t *testing.T) {
	m := Get()

	states := []string{"queued", "pending", "running", "leased", "succeeded", "failed", "cancelled", "preempted", "rejected"}
	for _, state := range states {
		t.Run(state, func(t *testing.T) {
			before := testutil.ToFloat64(stateUpdatesCounter.WithLabelValues(state))
			m.RecordStateUpdates(state, 3)
			after := testutil.ToFloat64(stateUpdatesCounter.WithLabelValues(state))
			assert.Equal(t, float64(3), after-before)
		})
	}
}

func TestRecordTerminalStateUpdatesTotal(t *testing.T) {
	m := Get()

	before := testutil.ToFloat64(terminalStateUpdatesTotalCounter)
	m.RecordTerminalStateUpdatesTotal(5)
	after := testutil.ToFloat64(terminalStateUpdatesTotalCounter)
	assert.Equal(t, float64(5), after-before)
}
