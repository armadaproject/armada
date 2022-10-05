package schedulerobjects

import (
	"fmt"
	"time"

	resource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/pkg/api"
)

func NewNodeFromNodeInfo(nodeInfo *api.NodeInfo, executor string, allowedPriorities []int32) *Node {
	nodeType := NewNodeTypeFromNodeInfo(nodeInfo, nil, nil, nil)
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

// PodRequirementsMet determines whether a pod can be scheduled on this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
func (node *Node) PodRequirementsMet(req *PodRequirements, assignedResources AssignedByPriorityAndResourceType) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := PodRequirementsMet(node.GetTaints(), node.GetLabels(), req)
	if matches == false || err != nil {
		return matches, 0, reason, err
	}

	// Check if the pod can be scheduled without preemption.
	canSchedule := true
	available := resource.Quantity{}
	for resource, required := range req.ResourceRequirements.Requests {
		q := node.AvailableQuantityByPriorityAndResource(0, string(resource))
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(0, string(resource)))
		if required.Cmp(available) == 1 {
			canSchedule = false
			break
		}
	}
	if canSchedule {
		return true, 1, nil, nil
	}

	// Check if the pod can be scheduled with preemption.
	for resource, required := range req.ResourceRequirements.Requests {
		q := node.AvailableQuantityByPriorityAndResource(req.Priority, string(resource))
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(req.Priority, string(resource)))
		if required.Cmp(available) == 1 {
			return false, 0, &InsufficientResources{
				Resource:  string(resource),
				Required:  required,
				Available: available,
			}, nil
		}
	}

	return true, 0, nil, nil
}

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	if node.AvailableByPriorityAndResource == nil {
		return resource.MustParse("0")
	}
	return AvailableByPriorityAndResourceType(node.AvailableByPriorityAndResource).Get(priority, resourceType)
}
