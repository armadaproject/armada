package executorapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestComputeResourceFromProtoResources(t *testing.T) {
	oneCpu := resource.MustParse("1")
	oneGi := resource.MustParse("1Gi")

	input := map[string]resource.Quantity{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1Gi"),
	}

	expected := &ComputeResource{
		Resources: map[string]*resource.Quantity{
			"cpu":    &oneCpu,
			"memory": &oneGi,
		},
	}
	actual := ComputeResourceFromProtoResources(input)
	assert.Equal(t, actual, expected)
}

func TestResourceListFromProtoResources(t *testing.T) {
	oneCpu := resource.MustParse("1")
	oneGi := resource.MustParse("1Gi")

	input := map[string]*resource.Quantity{
		"cpu":    &oneCpu,
		"memory": &oneGi,
	}
	expected := schedulerobjects.ResourceList{
		Resources: map[string]*resource.Quantity{
			"cpu":    &oneCpu,
			"memory": &oneGi,
		},
	}

	actual := ResourceListFromProtoResources(input)
	assert.Equal(t, actual.Resources, expected.Resources)
}

func TestNewNodeFromNodeInfoCopiesNodeTaints(t *testing.T) {
	taint := &v1.Taint{Key: "example.com/dedicated", Value: "gpu", Effect: v1.TaintEffectNoSchedule}
	userFacingTaint := &v1.Taint{Key: "example.com/legacy", Value: "true", Effect: v1.TaintEffectNoSchedule}
	nodeInfo := &NodeInfo{
		Name:   "node-1",
		Taints: []*v1.Taint{taint, userFacingTaint},
		NodeTaints: []*NodeTaint{
			{Taint: taint, Category: "platform", UserFacing: false},
			{Taint: userFacingTaint, UserFacing: true},
		},
	}

	node, err := NewNodeFromNodeInfo(nodeInfo, "executor-1", nil, time.Now())
	require.NoError(t, err)
	assert.Equal(t, nodeInfo.Taints, node.Taints)
	assert.Equal(t, []*schedulerobjects.NodeTaint{
		{Taint: taint, Category: "platform", UserFacing: false},
		{Taint: userFacingTaint, UserFacing: true},
	}, node.NodeTaints)
}
