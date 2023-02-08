package schedulerobjects

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	if node.AllocatableByPriorityAndResource == nil {
		return resource.Quantity{}
	}
	return AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).Get(priority, resourceType)
}

func (node *Node) DeepCopy() *Node {
	allocatedByJobId := maps.Clone(node.AllocatedByJobId)
	for jobId, rl := range allocatedByJobId {
		allocatedByJobId[jobId] = rl.DeepCopy()
	}
	return &Node{
		Id:             node.Id,
		LastSeen:       node.LastSeen,
		NodeType:       node.NodeType.DeepCopy(),
		NodeTypeId:     node.NodeTypeId,
		Taints:         slices.Clone(node.Taints),
		Labels:         maps.Clone(node.Labels),
		TotalResources: node.TotalResources.DeepCopy(),
		JobRunsByState: maps.Clone(node.JobRunsByState),
		AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).DeepCopy(),
		AllocatedByJobId: allocatedByJobId,
	}
}
