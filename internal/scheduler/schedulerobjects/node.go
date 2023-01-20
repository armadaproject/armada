package schedulerobjects

import (
	"fmt"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	resource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
)

func NewNodeFromNodeInfo(nodeInfo *api.NodeInfo, executor string, allowedPriorities []int32) *Node {
	allocatableByPriorityAndResource := NewAllocatableByPriorityAndResourceType(allowedPriorities, nodeInfo.TotalResources)
	for p, rs := range nodeInfo.AllocatedResources {
		allocatableByPriorityAndResource.MarkAllocated(p, ResourceList{Resources: rs.Resources})
	}
	return &Node{
		Id:                               fmt.Sprintf("%s-%s", executor, nodeInfo.Name),
		LastSeen:                         time.Now(),
		Taints:                           nodeInfo.GetTaints(),
		Labels:                           nodeInfo.GetLabels(),
		TotalResources:                   ResourceList{Resources: nodeInfo.TotalResources},
		AllocatableByPriorityAndResource: allocatableByPriorityAndResource,
	}
}

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	if node.AllocatableByPriorityAndResource == nil {
		return resource.Quantity{}
	}
	return AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).Get(priority, resourceType)
}

func (node *Node) DeepCopy() *Node {
	return &Node{
		Id:             node.Id,
		LastSeen:       node.LastSeen,
		NodeType:       node.NodeType.DeepCopy(),
		NodeTypeId:     node.NodeTypeId,
		Taints:         slices.Clone(node.Taints),
		Labels:         maps.Clone(node.Labels),
		TotalResources: node.TotalResources.DeepCopy(),
		AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).DeepCopy(),
	}
}
