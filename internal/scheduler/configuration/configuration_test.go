package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

func TestGetProtectedFractionOfFairShare(t *testing.T) {
	zero := 0.0
	half := 0.5
	sc := SchedulingConfig{
		ProtectedFractionOfFairShare: 0.1,
		Pools: []PoolConfig{
			{
				Name:                         "overrides-pool",
				ProtectedFractionOfFairShare: &half,
			},
			{
				Name:                         "overrides-zero-pool",
				ProtectedFractionOfFairShare: &zero,
			},
			{
				Name: "not-set-pool",
			},
		},
	}

	assert.Equal(t, 0.5, sc.GetProtectedFractionOfFairShare("overrides-pool"))
	assert.Equal(t, 0.0, sc.GetProtectedFractionOfFairShare("overrides-zero-pool"))
	assert.Equal(t, 0.1, sc.GetProtectedFractionOfFairShare("not-set-pool"))
	assert.Equal(t, 0.1, sc.GetProtectedFractionOfFairShare("missing-pool"))
}

func TestApplyRespectNodePodLimits(t *testing.T) {
	cpu := ResourceType{Name: "cpu", Resolution: resource.MustParse("1m")}
	mem := ResourceType{Name: "memory", Resolution: resource.MustParse("1")}
	podsDefault := ResourceType{Name: armadaresource.PodsResourceName, Resolution: resource.MustParse("1")}
	podsCustom := ResourceType{Name: armadaresource.PodsResourceName, Resolution: resource.MustParse("2")}

	tests := map[string]struct {
		initial         SchedulingConfig
		applyTimes      int
		expectApplied   bool
		expectSupported []ResourceType
		expectIndexed   []ResourceType
	}{
		"flag off leaves config untouched": {
			initial: SchedulingConfig{
				SupportedResourceTypes: []ResourceType{cpu, mem},
				IndexedResources:       []ResourceType{cpu, mem},
			},
			applyTimes:      1,
			expectApplied:   false,
			expectSupported: []ResourceType{cpu, mem},
			expectIndexed:   []ResourceType{cpu, mem},
		},
		"flag on appends pods to both slices with default resolution": {
			initial: SchedulingConfig{
				RespectNodePodLimits:   true,
				SupportedResourceTypes: []ResourceType{cpu, mem},
				IndexedResources:       []ResourceType{cpu},
			},
			applyTimes:      1,
			expectApplied:   true,
			expectSupported: []ResourceType{cpu, mem, podsDefault},
			expectIndexed:   []ResourceType{cpu, podsDefault},
		},
		"flag on normalizes caller-supplied pods resolution to 1": {
			// jobdb injects pods=1 per job; any resolution other than 1 would break
			// 1-to-1 pod accounting, so ensurePodsResourceType rewrites the entry.
			initial: SchedulingConfig{
				RespectNodePodLimits:   true,
				SupportedResourceTypes: []ResourceType{cpu, podsCustom},
				IndexedResources:       []ResourceType{cpu, podsCustom},
			},
			applyTimes:      1,
			expectApplied:   true,
			expectSupported: []ResourceType{cpu, podsDefault},
			expectIndexed:   []ResourceType{cpu, podsDefault},
		},
		"idempotent on repeated calls": {
			initial: SchedulingConfig{
				RespectNodePodLimits:   true,
				SupportedResourceTypes: []ResourceType{cpu},
				IndexedResources:       []ResourceType{cpu},
			},
			applyTimes:      3,
			expectApplied:   true,
			expectSupported: []ResourceType{cpu, podsDefault},
			expectIndexed:   []ResourceType{cpu, podsDefault},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sc := tc.initial
			var applied bool
			for i := 0; i < tc.applyTimes; i++ {
				applied = ApplyRespectNodePodLimits(&sc)
			}
			assert.Equal(t, tc.expectApplied, applied)
			assertResourceTypesEqual(t, tc.expectSupported, sc.SupportedResourceTypes)
			assertResourceTypesEqual(t, tc.expectIndexed, sc.IndexedResources)
		})
	}
}

func assertResourceTypesEqual(t *testing.T, expected, actual []ResourceType) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i := range expected {
		assert.Equal(t, expected[i].Name, actual[i].Name, "index %d name", i)
		assert.True(t, expected[i].Resolution.Equal(actual[i].Resolution),
			"index %d (%s): expected resolution %s, got %s",
			i, expected[i].Name, expected[i].Resolution.String(), actual[i].Resolution.String())
	}
}
