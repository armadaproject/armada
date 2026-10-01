package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func TestClusterTarget_ShouldEvaluateReadiness(t *testing.T) {
	require.True(t, (&ClusterTarget{}).ShouldEvaluateReadiness(), "external cluster defaults to probing")
	require.True(t, (&ClusterTarget{Kind: true}).ShouldEvaluateReadiness())
	require.False(t, (&ClusterTarget{EvaluateReadiness: boolPtr(false)}).ShouldEvaluateReadiness())
	require.True(t, (&ClusterTarget{Kind: true, EvaluateReadiness: boolPtr(true)}).ShouldEvaluateReadiness())
}

func TestClusterTarget_ShouldReadinessSelectTarget(t *testing.T) {
	require.False(t, (&ClusterTarget{}).ShouldReadinessSelectTarget(), "external cluster defaults to annotation-only")
	require.True(t, (&ClusterTarget{Kind: true}).ShouldReadinessSelectTarget(), "kind defaults to selecting the target")
	require.True(t, (&ClusterTarget{ReadinessSelectsTarget: boolPtr(true)}).ShouldReadinessSelectTarget())
	require.False(t, (&ClusterTarget{Kind: true, ReadinessSelectsTarget: boolPtr(false)}).ShouldReadinessSelectTarget())
}

func TestClusterTarget_EffectiveReadinessSettings(t *testing.T) {
	unset := &ClusterTarget{}
	require.Equal(t, DefaultReadinessRetries, unset.EffectiveReadinessRetries())
	require.Equal(t, DefaultReadinessDelay, unset.EffectiveReadinessDelay())

	set := &ClusterTarget{ReadinessRetries: 7, ReadinessDelayDuration: 3 * time.Second}
	require.Equal(t, 7, set.EffectiveReadinessRetries())
	require.Equal(t, 3*time.Second, set.EffectiveReadinessDelay())
}
