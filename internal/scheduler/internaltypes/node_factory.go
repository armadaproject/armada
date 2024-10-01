package internaltypes

import (
	"github.com/armadaproject/armada/internal/common/util"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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

func (f *NodeFactory) FromSchedulerObjectsNode(node *schedulerobjects.Node) (*Node, error) {
	f.nodeIndexCounter++
	return FromSchedulerObjectsNode(node,
		f.nodeIndexCounter,
		f.indexedTaints,
		f.indexedNodeLabels,
		f.resourceListFactory,
	)
}
