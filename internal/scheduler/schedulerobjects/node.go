package schedulerobjects

import (
	"fmt"

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
		NodeType:       node.NodeType.DeepCopy(),
		Taints:         slices.Clone(node.Taints),
		Labels:         maps.Clone(node.Labels),
		TotalResources: node.TotalResources.DeepCopy(),
		AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).DeepCopy(),
		StateByJobRunId:             maps.Clone(node.StateByJobRunId),
		NonArmadaAllocatedResources: armadamaps.DeepCopy(node.NonArmadaAllocatedResources),
		Unschedulable:               node.Unschedulable,
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
	for _, rl := range node.NonArmadaAllocatedResources {
		tr.Sub(rl)
	}
	return tr
}
