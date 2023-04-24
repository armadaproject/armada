package schedulerobjects

import (
	"fmt"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

func (node *Node) AvailableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	return AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).Get(priority, resourceType)
}

// DominantQueue returns the name of the queue with largest CPU request on this node.
// If a tie, the lexicographically smaller queue is returned.
func (node *Node) DominantQueue() string {
	dominantQueue := ""
	dominantQueueCombinedResources := 0.0
	for queue, rl := range node.AllocatedByQueue {
		v := armadaresource.QuantityAsFloat64(rl.Get("cpu"))
		if dominantQueue == "" || v > dominantQueueCombinedResources || (v == dominantQueueCombinedResources && queue < dominantQueue) {
			dominantQueue = queue
			dominantQueueCombinedResources = v
		}
	}
	return dominantQueue
}

// NumActiveQueues returns the number of queues requesting a non-zero amount of resources on the node.
func (node *Node) NumActiveQueues() int {
	rv := 0
	for _, rl := range node.AllocatedByQueue {
		if !rl.IsZero() {
			rv++
		}
	}
	return rv
}

func (node *Node) DeepCopy() *Node {
	if node == nil {
		return nil
	}
	return &Node{
		Id:             node.Id,
		Name:           node.Name,
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
	}
}

func (node *Node) CompactString() string {
	if node == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Node{Id; %s}", node.Id)
}
