package schedulerobjects

import (
	"fmt"
	"time"

	resource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/pkg/api"
)

func NewNodeFromNodeInfo(nodeInfo *api.NodeInfo, executor string, allowedPriorities []int32, indexedTaints map[string]interface{}, indexedLabels map[string]interface{}) *Node {
	nodeType := NewNodeTypeFromNodeInfo(nodeInfo, indexedTaints, indexedLabels)
	availableByPriorityAndResource := NewAvailableByPriorityAndResourceType(allowedPriorities, nodeInfo.TotalResources)
	for p, rs := range nodeInfo.AllocatedResources {
		availableByPriorityAndResource.MarkUsed(p, ResourceList{Resources: rs.Resources})
	}
	return &Node{
		Id:                             fmt.Sprintf("%s-%s", executor, nodeInfo.Name),
		LastSeen:                       time.Now(),
		NodeType:                       nodeType,
		NodeTypeId:                     nodeType.Id,
		Taints:                         nodeInfo.GetTaints(),
		Labels:                         nodeInfo.GetLabels(),
		TotalResources:                 ResourceList{Resources: nodeInfo.TotalResources},
		AvailableByPriorityAndResource: availableByPriorityAndResource,
	}
}

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	if node.AvailableByPriorityAndResource == nil {
		return resource.MustParse("0")
	}
	return AvailableByPriorityAndResourceType(node.AvailableByPriorityAndResource).Get(priority, resourceType)
}
