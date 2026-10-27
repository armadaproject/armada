package internaltypes

import (
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/types"
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

	// Allowed priorities, includes home and away (from config)
	allowedPriorities []int32

	// Factory for internaltypes.ResourceList
	resourceListFactory *ResourceListFactory

	// Used for assigning node index
	nodeIndexCounter atomic.Uint64
}

func NewNodeFactory(
	indexedTaints []string,
	indexedNodeLabels []string,
	priorityClasses map[string]types.PriorityClass,
	resourceListFactory *ResourceListFactory,
) *NodeFactory {
	return &NodeFactory{
		indexedTaints:       util.StringListToSet(indexedTaints),
		indexedNodeLabels:   util.StringListToSet(indexedNodeLabels),
		allowedPriorities:   types.AllowedPriorities(priorityClasses),
		resourceListFactory: resourceListFactory,
		nodeIndexCounter:    atomic.Uint64{},
	}
}

func (f *NodeFactory) CreateNodeAndType(
	id string,
	executor string,
	name string,
	pool string,
	reportingNodeType string,
	unschedulable bool,
	taints []v1.Taint,
	labels map[string]string,
	totalResources ResourceList,
	allocatableResources ResourceList,
) *Node {
	return CreateNodeAndType(
		id,
		f.allocateNodeIndex(),
		executor,
		name,
		pool,
		reportingNodeType,
		unschedulable,
		taints,
		labels,
		f.indexedTaints,
		f.indexedNodeLabels,
		totalResources,
		allocatableResources,
		f.allowedPriorities,
	)
}

func (f *NodeFactory) FromSchedulerObjectsNode(node *schedulerobjects.Node) *Node {
	return FromSchedulerObjectsNode(node,
		f.allocateNodeIndex(),
		f.indexedTaints,
		f.indexedNodeLabels,
		f.allowedPriorities,
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
			result = append(result, f.FromSchedulerObjectsNode(node))
		}
	}
	return result
}

func (f *NodeFactory) ResourceListFactory() *ResourceListFactory {
	return f.resourceListFactory
}

func (f *NodeFactory) AddLabels(nodes []*Node, extraLabels map[string]string) []*Node {
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		result[i] = node.WithLabels(util.MergeMaps(node.GetLabels(), extraLabels))
	}
	return result
}

func (f *NodeFactory) AddTaints(nodes []*Node, extraTaints []v1.Taint) []*Node {
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		result[i] = node.WithTaints(append(node.GetTaints(), extraTaints...))
	}
	return result
}

// RemoveCordonTaint returns copies of nodes with cordon taints stripped (see IsCordonTaint) and the
// unschedulable flag cleared. The submit checker uses it so that jobs targeting a node type whose
// nodes are all cordoned (via `kubectl cordon`, or otherwise reported unschedulable by the executor)
// stay queued rather than being rejected. See issue #4946.
//
// A cordoned node always has the unschedulable flag set (Kubernetes only adds
// node.kubernetes.io/unschedulable when the node is unschedulable, and the Armada taint is only
// synthesized for unschedulable nodes), so the flag alone identifies the nodes to fix.
//
// WithSchedulable only strips the Armada taint, so we strip the whole cordon taint set here
// and let WithSchedulable clear the flag.
func (f *NodeFactory) RemoveCordonTaint(nodes []*Node) []*Node {
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		if !node.IsUnschedulable() {
			// Not cordoned. Freshly built node, not yet inserted into a NodeDb, so reuse it as-is.
			result[i] = node
			continue
		}
		taints := node.GetTaints()
		nonCordonTaints := make([]v1.Taint, 0, len(taints))
		for _, taint := range taints {
			if !IsCordonTaint(taint) {
				nonCordonTaints = append(nonCordonTaints, taint)
			}
		}
		result[i] = node.WithTaints(nonCordonTaints).WithSchedulable(true)
	}
	return result
}

func (f *NodeFactory) allocateNodeIndex() uint64 {
	return f.nodeIndexCounter.Add(1)
}
