package logging

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestPrometheusHook_IncrementsCounter verifies that calling Run on monitored levels
// properly increments the corresponding counter.
func TestPrometheusHook_IncrementsCounter(t *testing.T) {
	hook := NewPrometheusHook()

	// Clean up: Unregister the counters to avoid polluting the global registry in further tests.
	t.Cleanup(func() {
		for _, counter := range hook.counters {
			prometheus.Unregister(counter)
		}
	})

	// Define the log levels we are monitoring.
	levels := []zerolog.Level{
		zerolog.DebugLevel,
		zerolog.InfoLevel,
		zerolog.WarnLevel,
		zerolog.ErrorLevel,
	}

	for _, level := range levels {
		n := 3
		for i := 0; i < n; i++ {
			hook.Run(nil, level, "dummy message")
		}
		// Verify the counter value.
		value := testutil.ToFloat64(hook.counters[level])
		assert.Equal(t, float64(n), value, "expected counter for level %s to be %d, got %f", level, n, value)
	}
}
