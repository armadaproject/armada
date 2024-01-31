package healthmonitor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManualHealthMonitor(t *testing.T) {
	hm := NewManualHealthMonitor()

	ok, reason, err := hm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm.SetHealthStatus(true)
	ok, reason, err = hm.IsHealthy()
	require.True(t, ok)
	require.Empty(t, reason)
	require.NoError(t, err)

	hm.SetHealthStatus(false)
	ok, reason, err = hm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm.SetHealthStatus(false)
	hm = hm.WithReason("foo")
	ok, reason, err = hm.IsHealthy()
	require.False(t, ok)
	require.Equal(t, "foo", reason)
	require.NoError(t, err)
}
