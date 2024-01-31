package healthmonitor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiHealthMonitor_OneChild(t *testing.T) {
	hm := NewManualHealthMonitor()
	mhm := NewMultiHealthMonitor("mhm", map[string]HealthMonitor{"hm": hm})
	ok, reason, err := mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm.SetHealthStatus(true)
	ok, reason, err = mhm.IsHealthy()
	require.True(t, ok)
	require.Empty(t, reason)
	require.NoError(t, err)
}

func TestMultiHealthMonitor_TwoChildren(t *testing.T) {
	hm1 := NewManualHealthMonitor()
	hm2 := NewManualHealthMonitor()
	mhm := NewMultiHealthMonitor("mhm", map[string]HealthMonitor{"hm1": hm1, "hm2": hm2})

	ok, reason, err := mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm1.SetHealthStatus(true)
	ok, reason, err = mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm1.SetHealthStatus(false)
	hm2.SetHealthStatus(true)
	ok, reason, err = mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm1.SetHealthStatus(true)
	ok, reason, err = mhm.IsHealthy()
	require.True(t, ok)
	require.Empty(t, reason)
	require.NoError(t, err)
}

func TestMultiHealthMonitor_TwoChildrenTimeouts(t *testing.T) {
	hm1 := NewManualHealthMonitor()
	hm1 = hm1.WithReason(UnavailableReason)
	hm2 := NewManualHealthMonitor()
	mhm := NewMultiHealthMonitor("mhm", map[string]HealthMonitor{"hm1": hm1, "hm2": hm2})
	mhm = mhm.WithMinimumReplicasAvailable(1)

	hm1.SetHealthStatus(false)
	hm2.SetHealthStatus(true)
	ok, reason, err := mhm.IsHealthy()
	require.True(t, ok)
	require.Empty(t, reason)
	require.NoError(t, err)

	hm1.SetHealthStatus(true)
	hm2.SetHealthStatus(false)
	ok, reason, err = mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)

	hm1.SetHealthStatus(false)
	hm2.SetHealthStatus(false)
	ok, reason, err = mhm.IsHealthy()
	require.False(t, ok)
	require.NotEmpty(t, reason)
	require.NoError(t, err)
}
