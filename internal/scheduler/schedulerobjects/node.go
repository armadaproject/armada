package schedulerobjects

import (
	"fmt"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
)

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	return AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).Get(priority, resourceType)
}

func (node *Node) DeepCopy() *Node {
	if node == nil {
		return nil
	}
	return &Node{
		Id:             node.Id,
		Name:           node.Name,
		NodeDbKeys:     slices.Clone(node.NodeDbKeys),
		LastSeen:       node.LastSeen,
		NodeType:       node.NodeType.DeepCopy(),
		NodeTypeId:     node.NodeTypeId,
		Taints:         slices.Clone(node.Taints),
		Labels:         maps.Clone(node.Labels),
		TotalResources: node.TotalResources.DeepCopy(),
		AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).DeepCopy(),
		StateByJobRunId:             maps.Clone(node.StateByJobRunId),
		AllocatedByJobId:            armadamaps.DeepCopy(node.AllocatedByJobId),
		AllocatedByQueue:            armadamaps.DeepCopy(node.AllocatedByQueue),
		EvictedJobRunIds:            maps.Clone(node.EvictedJobRunIds),
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
