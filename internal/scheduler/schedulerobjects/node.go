package schedulerobjects

import (
	"fmt"
	"math"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
)

func (node *Node) DeepCopy() *Node {
	if node == nil {
		return nil
	}
	return &Node{
		Id:             node.Id,
		Name:           node.Name,
		Executor:       node.Executor,
		LastSeen:       node.LastSeen,
		Taints:         slices.Clone(node.Taints),
		Labels:         maps.Clone(node.Labels),
		TotalResources: node.TotalResources.DeepCopy(),
		AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).DeepCopy(),
		StateByJobRunId:        maps.Clone(node.StateByJobRunId),
		UnallocatableResources: armadamaps.DeepCopy(node.UnallocatableResources),
		Unschedulable:          node.Unschedulable,
	}
}

func (node *Node) CompactString() string {
	if node == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Node{Id; %s}", node.Id)
}

func (node *Node) AvailableArmadaResource() ResourceList {
	tr := node.TotalResources.DeepCopy()
	for _, rl := range node.UnallocatableResources {
		tr.Sub(rl)
	}
	return tr
}

func (node *Node) MarkResourceUnallocatable(unallocatable ResourceList) {
	currentAllocatable := node.UnallocatableResources[math.MaxInt32]
	(&currentAllocatable).Add(unallocatable)
	node.UnallocatableResources[math.MaxInt32] = currentAllocatable

	for priority, allocatable := range node.AllocatableByPriorityAndResource {
		allocatable.Sub(unallocatable)
		allocatable.LimitToZero()
		node.AllocatableByPriorityAndResource[priority] = allocatable
	}
}
