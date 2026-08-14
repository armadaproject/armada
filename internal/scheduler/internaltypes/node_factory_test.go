package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/types"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
)

func newTestNodeFactory(t *testing.T) *NodeFactory {
	rlf, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
			{Name: "memory", Resolution: resource.MustParse("1")},
		},
		nil,
	)
	require.NoError(t, err)
	return NewNodeFactory(
		[]string{"existing"},
		[]string{},
		map[string]types.PriorityClass{},
		rlf,
	)
}

func TestNodeFactory_WithTaints(t *testing.T) {
	f := newTestNodeFactory(t)
	rl := ResourceList{}

	original := f.CreateNodeAndType(
		"node-1", "executor-1", "node-1", "away-pool", "type",
		false,
		[]v1.Taint{{Key: "existing", Value: "1", Effect: v1.TaintEffectNoSchedule}},
		map[string]string{"lbl": "a"},
		rl, rl, map[int32]ResourceList{},
	)

	newTaints := []v1.Taint{{Key: "existing", Value: "2", Effect: v1.TaintEffectNoSchedule}}
	updated := f.WithTaints(original, newTaints)

	// New taint set is applied.
	assert.Equal(t, newTaints, updated.GetTaints())
	// Node type is recomputed: different indexed taint value => different node type id.
	assert.NotEqual(t, original.GetNodeTypeId(), updated.GetNodeTypeId())
	// Identity fields preserved.
	assert.Equal(t, original.GetId(), updated.GetId())
	assert.Equal(t, original.GetPool(), updated.GetPool())
	assert.Equal(t, original.GetLabels(), updated.GetLabels())
	// Original is untouched.
	assert.Equal(t, []v1.Taint{{Key: "existing", Value: "1", Effect: v1.TaintEffectNoSchedule}}, original.GetTaints())
}

func TestNodeFactory_WithTaints_Empty(t *testing.T) {
	f := newTestNodeFactory(t)
	rl := ResourceList{}
	original := f.CreateNodeAndType(
		"node-1", "executor-1", "node-1", "away-pool", "type",
		false,
		[]v1.Taint{{Key: "existing", Value: "1", Effect: v1.TaintEffectNoSchedule}},
		map[string]string{}, rl, rl, map[int32]ResourceList{},
	)

	updated := f.WithTaints(original, []v1.Taint{})
	assert.Empty(t, updated.GetTaints())
}

func countUnschedulableTaints(taints []v1.Taint) int {
	count := 0
	for _, taint := range taints {
		if taint.Key == UnschedulableTaint().Key {
			count++
		}
	}
	return count
}

func TestNodeFactory_WithTaints_UnschedulableFlagGoverned(t *testing.T) {
	tests := map[string]struct {
		// inputTaints derives the taint list passed to WithTaints from the original node's taints.
		inputTaints func(original []v1.Taint) []v1.Taint
	}{
		"no-op pass does not duplicate the unschedulable taint": {
			// Pass the node's own taints (which include the synthesized taint) straight back in.
			inputTaints: func(original []v1.Taint) []v1.Taint { return original },
		},
		"delete of the unschedulable taint is re-added by the flag": {
			// Simulate a Delete modification targeting the unschedulable key, as calculateTaints
			// would produce: the incoming list has already had the unschedulable taint stripped.
			inputTaints: func(original []v1.Taint) []v1.Taint {
				result := make([]v1.Taint, 0, len(original))
				for _, taint := range original {
					if taint.Key != UnschedulableTaint().Key {
						result = append(result, taint)
					}
				}
				return result
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := newTestNodeFactory(t)
			rl := ResourceList{}
			original := f.CreateNodeAndType(
				"node-1", "executor-1", "node-1", "away-pool", "type",
				true,
				[]v1.Taint{{Key: "existing", Value: "1", Effect: v1.TaintEffectNoSchedule}},
				map[string]string{"lbl": "a"},
				rl, rl, map[int32]ResourceList{},
			)
			// Sanity check: the freshly built unschedulable node has exactly one synthesized
			// unschedulable taint.
			require.Equal(t, 1, countUnschedulableTaints(original.GetTaints()))

			rebuilt := f.WithTaints(original, tc.inputTaints(original.GetTaints()))

			// The unschedulable taint is governed solely by the flag: present exactly once,
			// never duplicated and never removable via the taint list.
			assert.Equal(t, 1, countUnschedulableTaints(rebuilt.GetTaints()))
			assert.True(t, rebuilt.IsUnschedulable())
		})
	}
}
