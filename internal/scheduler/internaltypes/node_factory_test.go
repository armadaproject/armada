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
		{taint: existing, category: "user", userFacing: true},
		{taint: added},
	}, result[0].GetNodeTaints())
	assert.True(t, result[0].hasCompleteNodeTaintMetadata)
	assert.Equal(t, originalTaints, node.GetTaints())
	assert.Equal(t, originalMetadata, node.GetNodeTaints())

	// Metadata-aware filtering removes the neutral taint but keeps the user-facing taint.
	filtered := factory.RemoveCordonTaints(result)
	assert.Equal(t, []v1.Taint{existing}, filtered[0].GetTaints())
	assert.Equal(t, []NodeTaint{
		{taint: existing, category: "user", userFacing: true},
	}, filtered[0].GetNodeTaints())

	// Neither representation may retain mutable timestamps owned by the caller.
	extraTaints[0].TimeAdded.Time = time.Unix(2, 0)
	assert.Equal(t, added, result[0].GetTaints()[2])
	require.Len(t, result[0].GetNodeTaints(), 2)
	assert.Equal(t, added, result[0].GetNodeTaints()[1].GetTaint())
}

func TestRemoveCordonTaintsSelectsPerNodeFlow(t *testing.T) {
	cordon := v1.Taint{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}
	restricted := v1.Taint{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}
	user := v1.Taint{Key: "user", Effect: v1.TaintEffectNoSchedule}
	admin := v1.Taint{Key: "maintenance", Effect: v1.TaintEffectNoSchedule}
	legacyUserFacing := v1.Taint{Key: "legacy", Effect: v1.TaintEffectNoSchedule}
	tests := map[string]struct {
		taints         []*v1.Taint
		metadata       []*schedulerobjects.NodeTaint
		schedulable    bool
		expectOriginal bool
		wantTaints     []v1.Taint
		wantMetadata   []NodeTaint
	}{
		"legacy report preserves non-cordon restrictions": {
			taints:     []*v1.Taint{&cordon, &restricted},
			wantTaints: []v1.Taint{restricted},
		},
		"complete reordered metadata uses new flow": {
			taints: []*v1.Taint{&cordon, &restricted, &user, &admin},
			metadata: []*schedulerobjects.NodeTaint{
				{Taint: &admin, Category: "platform"},
				{Taint: &user, Category: "user", UserFacing: true},
				{Taint: &restricted},
				{Taint: &cordon},
			},
			wantTaints: []v1.Taint{user},
			wantMetadata: []NodeTaint{
				{taint: user, category: "user", userFacing: true},
			},
		},
		"partial metadata uses legacy flow": {
			taints:       []*v1.Taint{&cordon, &restricted, &user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, Category: "user", UserFacing: true}},
			wantTaints:   []v1.Taint{restricted, user},
			wantMetadata: []NodeTaint{{taint: user, category: "user", userFacing: true}},
		},
		"schedulable user-facing taint is kept": {
			taints:       []*v1.Taint{&user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, Category: "user", UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{user},
			wantMetadata: []NodeTaint{{taint: user, category: "user", userFacing: true}},
		},
		"schedulable admin taint is removed": {
			taints:       []*v1.Taint{&admin},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &admin, Category: "platform"}},
			schedulable:  true,
			wantMetadata: []NodeTaint{},
		},
		"schedulable admin taint keeps user-facing taints": {
			taints:       []*v1.Taint{&admin, &user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &admin, Category: "platform"}, {Taint: &user, Category: "user", UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{user},
			wantMetadata: []NodeTaint{{taint: user, category: "user", userFacing: true}},
		},
		"schedulable neutral taint is removed": {
			taints:       []*v1.Taint{&restricted},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &restricted}},
			schedulable:  true,
			wantMetadata: []NodeTaint{},
		},
		"unschedulable neutral taint is removed": {
			taints:       []*v1.Taint{&restricted},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &restricted}},
			wantMetadata: []NodeTaint{},
		},
		"legacy master taint is kept with complete metadata": {
			taints: []*v1.Taint{
				{Key: "node-role.kubernetes.io/master", Effect: v1.TaintEffectNoSchedule},
			},
			metadata: []*schedulerobjects.NodeTaint{
				{Taint: &v1.Taint{Key: "node-role.kubernetes.io/master", Effect: v1.TaintEffectNoSchedule}},
			},
			wantTaints: []v1.Taint{{Key: "node-role.kubernetes.io/master", Effect: v1.TaintEffectNoSchedule}},
			wantMetadata: []NodeTaint{{
				taint: v1.Taint{Key: "node-role.kubernetes.io/master", Effect: v1.TaintEffectNoSchedule},
			}},
		},
		"control-plane taint is kept with complete metadata": {
			taints: []*v1.Taint{
				{Key: "node-role.kubernetes.io/control-plane", Effect: v1.TaintEffectNoSchedule},
			},
			metadata: []*schedulerobjects.NodeTaint{
				{Taint: &v1.Taint{Key: "node-role.kubernetes.io/control-plane", Effect: v1.TaintEffectNoSchedule}},
			},
			wantTaints: []v1.Taint{{Key: "node-role.kubernetes.io/control-plane", Effect: v1.TaintEffectNoSchedule}},
			wantMetadata: []NodeTaint{{
				taint: v1.Taint{Key: "node-role.kubernetes.io/control-plane", Effect: v1.TaintEffectNoSchedule},
			}},
		},
		"schedulable incomplete metadata is kept": {
			taints:         []*v1.Taint{&admin},
			schedulable:    true,
			expectOriginal: true,
			wantTaints:     []v1.Taint{admin},
		},
		"legacy user-facing taint is kept": {
			taints:       []*v1.Taint{&legacyUserFacing},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &legacyUserFacing, UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{legacyUserFacing},
			wantMetadata: []NodeTaint{{taint: legacyUserFacing, userFacing: true}},
		},
		"cordon taint is removed even when marked user-facing": {
			taints:       []*v1.Taint{&cordon},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &cordon, Category: "user", UserFacing: true}},
			schedulable:  true,
			wantMetadata: []NodeTaint{},
		},
		"empty report removes synthetic cordon taint": {},
	}
	resources, err := NewResourceListFactory([]configuration.ResourceType{{Name: "cpu", Resolution: resource.MustParse("1m")}}, nil)
	require.NoError(t, err)
	factory := NewNodeFactory(nil, nil, nil, resources)
	nodes := make([]*Node, 0, len(tests))
	indexByName := make(map[string]int, len(tests))
	for name, tc := range tests {
		node := factory.FromSchedulerObjectsNode(&schedulerobjects.Node{
			Id: name, Taints: tc.taints, NodeTaints: tc.metadata,
			Unschedulable: !tc.schedulable, TotalResources: &schedulerobjects.ResourceList{},
		})
		// Exercise the immutable copy path used by other node updates as well.
		indexByName[name] = len(nodes)
		nodes = append(nodes, node.WithLabels(map[string]string{"test": name}))
	}

	result := factory.RemoveCordonTaints(nodes)

	require.Len(t, result, len(tests))
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			i := indexByName[name]
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
