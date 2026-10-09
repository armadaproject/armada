package config

import (
	"os"
	"path/filepath"
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

func TestResolveTargetNodeGroups_RejectsOneProfileUsedByTwoMembers(t *testing.T) {
	dir := t.TempDir()
	writeProfile := func(file, name string) string {
		path := filepath.Join(dir, file)
		require.NoError(t, os.WriteFile(path, []byte("name: "+name+"\nallocatable:\n  cpu: \"4\"\n"), 0o600))
		return path
	}
	gpu, sameGpu, cpu := writeProfile("gpu.yaml", "gpu"), writeProfile("gpu-copy.yaml", "gpu"), writeProfile("cpu.yaml", "cpu")
	groups := map[string]NodeGroup{
		"a": {{NodeProfile: gpu, Count: 3}},
		"b": {{NodeProfile: sameGpu, Count: 2}},
		"c": {{NodeProfile: cpu, Count: 2}},
	}

	_, err := ResolveTargetNodeGroups(ExecutionTarget{Name: "t", NodeGroups: []string{"a", "b"}}, groups)
	require.ErrorContains(t, err, `node profile "gpu" is used by more than one`)

	members, err := ResolveTargetNodeGroups(ExecutionTarget{Name: "t", NodeGroups: []string{"a", "c"}}, groups)
	require.NoError(t, err)
	require.Len(t, members, 2)
}
