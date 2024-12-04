package internaltypes

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	v1 "k8s.io/api/core/v1"
)

type NodeFactory struct {
	// Taint keys that to create indexes for.
	// Should include taints frequently used for scheduling.
	// Since the NodeDb can efficiently sort out nodes with taints not tolerated
	// by a pod when looking for a node a pod can be scheduled on.
	//
	// If not set, all taints are indexed.
	indexedTaints map[string]bool

	// Node labels to create indexes for.
	// Should include node labels frequently used for scheduling.
	// Since the NodeDb can efficiently sort out nodes for which these labels
	// do not match pod node selectors when looking for a node a pod can be scheduled on.
	//
	// If not set, no labels are indexed.
	indexedNodeLabels map[string]bool

	// Factory for internaltypes.ResourceList
	resourceListFactory *ResourceListFactory

	// Used for assigning node index
	nodeIndexCounter uint64
}

func NewNodeFactory(
	indexedTaints []string,
	indexedNodeLabels []string,
	resourceListFactory *ResourceListFactory,
) *NodeFactory {
	return &NodeFactory{
		indexedTaints:       util.StringListToSet(indexedTaints),
		indexedNodeLabels:   util.StringListToSet(indexedNodeLabels),
		resourceListFactory: resourceListFactory,
		nodeIndexCounter:    0,
	}
}

func (f *NodeFactory) NewNode(node *schedulerobjects.Node,
	id string,
	executor string,
	name string,
	pool string,
	taints []v1.Taint,
	labels map[string]string,
	totalResources ResourceList,
	unallocatableResources map[int32]ResourceList,
	allocatableByPriority map[int32]ResourceList,
	allocatedByQueue map[string]ResourceList,
	allocatedByJobId map[string]ResourceList,
	evictedJobRunIds map[string]bool,
) *Node {

	nodeType := NewNodeType(
		taints,
		labels,
		f.indexedTaints,
		f.indexedNodeLabels,
	)

	f.nodeIndexCounter++

	return CreateNode(
		node.Id,
		nodeType,
		f.nodeIndexCounter,
		node.Executor,
		node.Name,
		node.Pool,
		taints,
		labels,
		totalResources,
		unallocatableResources,
		allocatableByPriority,
		allocatedByQueue,
		allocatedByJobId,
		evictedJobRunIds,
		nil,
	)
}

func (f *NodeFactory) FromSchedulerObjectsNode(node *schedulerobjects.Node) (*Node, error) {
	f.nodeIndexCounter++
	return FromSchedulerObjectsNode(node,
		f.nodeIndexCounter,
		f.indexedTaints,
		f.indexedNodeLabels,
		f.resourceListFactory,
	)
}

func (f *NodeFactory) FromSchedulerObjectsExecutors(executors []*schedulerobjects.Executor, errorLogger func(string)) []*Node {
	result := []*Node{}
	for _, executor := range executors {
		for _, node := range executor.GetNodes() {
			if executor.Id != node.Executor {
				errorLogger(fmt.Sprintf("Executor name mismatch: %q != %q", node.Executor, executor.Id))
				continue
			}
			itNode, err := f.FromSchedulerObjectsNode(node)
			if err != nil {
				errorLogger(fmt.Sprintf("Invalid node %s: %v", node.Name, err))
				continue
			}
			result = append(result, itNode)
		}
	}
	return result
}
