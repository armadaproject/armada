package testfixtures

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestSimpleNode(id string) *internaltypes.Node {
	return internaltypes.CreateNode(
		id,
		nil,
		0,
		"",
		"",
		"",
		nil,
		nil,
		internaltypes.ResourceList{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
}

func N32CpuNodes(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = Test32CpuNode(priorities)
	}
	return rv
}

func Test32CpuNode(priorities []int32) *schedulerobjects.Node {
	return TestNode(
		priorities,
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("32"),
			"memory": resource.MustParse("256Gi"),
		},
	)
}

func TestNode(priorities []int32, resources map[string]resource.Quantity, nodeFactory *internaltypes.NodeFactory) *internaltypes.Node {
	id := uuid.NewString()

	return internaltypes.CreateNode(
		id,
	)

	return &schedulerobjects.Node{
		Id:             id,
		Name:           id,
		Pool:           TestPool,
		TotalResources: schedulerobjects.ResourceList{Resources: resources},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			schedulerobjects.ResourceList{Resources: resources},
		),
		StateByJobRunId: make(map[string]schedulerobjects.JobRunState),
		Labels: map[string]string{
			TestHostnameLabel: id,
			// TODO(albin): Nodes should be created from the NodeDb to ensure this label is set automatically.
			schedulerconfiguration.NodeIdLabel: id,
		},
	}
}
