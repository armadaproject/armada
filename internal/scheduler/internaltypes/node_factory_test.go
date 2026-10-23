package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestRemoveCordonTaintsSelectsPerNodeFlow(t *testing.T) {
	cordon := v1.Taint{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}
	restricted := v1.Taint{Key: "dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}
	user := v1.Taint{Key: "user", Effect: v1.TaintEffectNoSchedule}
	admin := v1.Taint{Key: "maintenance", Effect: v1.TaintEffectNoSchedule}
	tests := []struct {
		name         string
		taints       []*v1.Taint
		metadata     []*schedulerobjects.NodeTaint
		schedulable  bool
		wantTaints   []v1.Taint
		wantMetadata []NodeTaint
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
				{Taint: &admin, Category: "platform"}, {Taint: &user, UserFacing: true},
				{Taint: &restricted}, {Taint: &cordon},
			},
			wantTaints:   []v1.Taint{restricted},
			wantMetadata: []NodeTaint{{Taint: restricted}},
		},
		{
			name:         "partial metadata uses legacy flow",
			taints:       []*v1.Taint{&cordon, &restricted, &user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, UserFacing: true}},
			wantTaints:   []v1.Taint{restricted, user},
			wantMetadata: []NodeTaint{{Taint: user, UserFacing: true}},
		},
		{
			name:         "schedulable node is reused",
			taints:       []*v1.Taint{&user},
			metadata:     []*schedulerobjects.NodeTaint{{Taint: &user, UserFacing: true}},
			schedulable:  true,
			wantTaints:   []v1.Taint{user},
			wantMetadata: []NodeTaint{{Taint: user, UserFacing: true}},
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
			if tc.schedulable {
				assert.Same(t, nodes[i], result[i])
			} else {
				assert.Contains(t, nodes[i].GetTaints(), UnschedulableTaint())
			}
			for _, taint := range tc.taints {
				assert.Contains(t, nodes[i].GetTaints(), *taint)
			}
		})
	}
}
