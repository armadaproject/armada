package internaltypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestAddTaintsPreservesMetadataAndAddsNeutralEntries(t *testing.T) {
	existing := v1.Taint{Key: "user", Effect: v1.TaintEffectNoSchedule}
	added := v1.Taint{
		Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule,
		TimeAdded: &metav1.Time{Time: time.Unix(1, 0)},
	}
	resources, err := NewResourceListFactory([]configuration.ResourceType{{Name: "cpu", Resolution: resource.MustParse("1m")}}, nil)
	require.NoError(t, err)
	factory := NewNodeFactory(nil, nil, nil, resources)
	node := factory.FromSchedulerObjectsNode(&schedulerobjects.Node{
		Taints:         []*v1.Taint{&existing},
		NodeTaints:     []*schedulerobjects.NodeTaint{{Taint: &existing, Category: "user", UserFacing: true}},
		Unschedulable:  true,
		TotalResources: &schedulerobjects.ResourceList{},
	})
	originalTaints := node.GetTaints()
	originalMetadata := node.GetNodeTaints()
	extraTaints := []v1.Taint{*added.DeepCopy()}

	result := factory.AddTaints([]*Node{node}, extraTaints)

	require.Len(t, result, 1)
	assert.Equal(t, []v1.Taint{existing, UnschedulableTaint(), added}, result[0].GetTaints())
	assert.Equal(t, []NodeTaint{
		{Taint: existing, Category: "user", UserFacing: true},
		{Taint: added},
	}, result[0].GetNodeTaints())
	assert.True(t, result[0].hasCompleteNodeTaintMetadata)
	assert.Equal(t, originalTaints, node.GetTaints())
	assert.Equal(t, originalMetadata, node.GetNodeTaints())

	// Added restrictions and user-facing taints must survive metadata-aware filtering.
	filtered := factory.RemoveCordonTaints(result)
	assert.Equal(t, []v1.Taint{existing, added}, filtered[0].GetTaints())
	assert.Equal(t, []NodeTaint{
		{Taint: existing, Category: UserFacingNodeTaintCategory, UserFacing: true},
		{Taint: added},
	}, filtered[0].GetNodeTaints())

	// Neither representation may retain mutable timestamps owned by the caller.
	extraTaints[0].TimeAdded.Time = time.Unix(2, 0)
	assert.Equal(t, added, result[0].GetTaints()[2])
	require.Len(t, result[0].GetNodeTaints(), 2)
	assert.Equal(t, added, result[0].GetNodeTaints()[1].Taint)
}

func TestRemoveCordonTaintsSelectsPerNodeFlow(t *testing.T) {
	cordon := v1.Taint{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}
	restricted := v1.Taint{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}
	user := v1.Taint{Key: "user", Effect: v1.TaintEffectNoSchedule}
	admin := v1.Taint{Key: "maintenance", Effect: v1.TaintEffectNoSchedule}
	legacyUserFacing := v1.Taint{Key: "legacy", Effect: v1.TaintEffectNoSchedule}
	tests := []struct {
		name           string
		taints         []*v1.Taint
		metadata       []*schedulerobjects.NodeTaint
		schedulable    bool
		expectOriginal bool
		wantTaints     []v1.Taint
		wantMetadata   []NodeTaint
	}{
		{
			name:       "legacy report preserves non-cordon restrictions",
			taints:     []*v1.Taint{&cordon, &restricted},
			wantTaints: []v1.Taint{restricted},
		},
		{
			name:   "complete reordered metadata uses new flow",
			taints: []*v1.Taint{&cordon, &restricted, &user, &admin},
			metadata: []*schedulerobjects.NodeTaint{
				{Taint: &admin, Category: "platform"},
				{Taint: &user, Category: UserFacingNodeTaintCategory, UserFacing: true},
				{Taint: &restricted},
				{Taint: &cordon},
			},
			wantTaints: []v1.Taint{restricted, user},
			wantMetadata: []NodeTaint{
				{Taint: restricted},
				{Taint: user, Category: UserFacingNodeTaintCategory, UserFacing: true},
			},
		},
		{
			name:         "partial metadata uses legacy flow",
			taints:       []*v1.Taint{&cordon, &restricted, &user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
			wantTaints:   []v1.Taint{restricted, user},
			wantMetadata: []NodeTaint{{Taint: user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
		},
		{
			name:         "schedulable user-facing taint is kept",
			taints:       []*v1.Taint{&user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{user},
			wantMetadata: []NodeTaint{{Taint: user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
		},
		{
			name:         "schedulable admin taint is removed",
			taints:       []*v1.Taint{&admin},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &admin, Category: "platform"}},
			schedulable:  true,
			wantMetadata: []NodeTaint{},
		},
		{
			name:         "schedulable admin taint keeps user-facing taints",
			taints:       []*v1.Taint{&admin, &user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &admin, Category: "platform"}, {Taint: &user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{user},
			wantMetadata: []NodeTaint{{Taint: user, Category: UserFacingNodeTaintCategory, UserFacing: true}},
		},
		{
			name:         "schedulable neutral taint is kept",
			taints:       []*v1.Taint{&restricted},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &restricted}},
			schedulable:  true,
			wantTaints:   []v1.Taint{restricted},
			wantMetadata: []NodeTaint{{Taint: restricted}},
		},
		{
			name:           "schedulable incomplete metadata is kept",
			taints:         []*v1.Taint{&admin},
			schedulable:    true,
			expectOriginal: true,
			wantTaints:     []v1.Taint{admin},
		},
		{
			name:         "legacy user-facing taint is kept",
			taints:       []*v1.Taint{&legacyUserFacing},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &legacyUserFacing, UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{legacyUserFacing},
			wantMetadata: []NodeTaint{{Taint: legacyUserFacing, UserFacing: true}},
		},
		{
			name:         "cordon taint is removed even when marked user-facing",
			taints:       []*v1.Taint{&cordon},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &cordon, Category: UserFacingNodeTaintCategory, UserFacing: true}},
			schedulable:  true,
			wantMetadata: []NodeTaint{},
		},
		{name: "empty report removes synthetic cordon taint"},
	}
	resources, err := NewResourceListFactory([]configuration.ResourceType{{Name: "cpu", Resolution: resource.MustParse("1m")}}, nil)
	require.NoError(t, err)
	factory := NewNodeFactory(nil, nil, nil, resources)
	nodes := make([]*Node, len(tests))
	for i, tc := range tests {
		nodes[i] = factory.FromSchedulerObjectsNode(&schedulerobjects.Node{
			Id: tc.name, Taints: tc.taints, NodeTaints: tc.metadata,
			Unschedulable: !tc.schedulable, TotalResources: &schedulerobjects.ResourceList{},
		})
		// Exercise the immutable copy path used by other node updates as well.
		nodes[i] = nodes[i].WithLabels(map[string]string{"test": tc.name})
	}

	result := factory.RemoveCordonTaints(nodes)

	require.Len(t, result, len(tests))
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.wantTaints, result[i].GetTaints())
			assert.ElementsMatch(t, tc.wantMetadata, result[i].GetNodeTaints())
			assert.False(t, result[i].IsUnschedulable())
			assert.Equal(t, !tc.schedulable, nodes[i].IsUnschedulable())
			if tc.expectOriginal {
				assert.Same(t, nodes[i], result[i])
			}
			if !tc.schedulable {
				assert.Contains(t, nodes[i].GetTaints(), UnschedulableTaint())
			}
			for _, taint := range tc.taints {
				assert.Contains(t, nodes[i].GetTaints(), *taint)
			}
		})
	}
}
