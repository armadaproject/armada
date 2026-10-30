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
	nodeTaints []NodeTaint,
	hasCompleteNodeTaintMetadata bool,
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
		nodeTaints,
		hasCompleteNodeTaintMetadata,
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
		nodeTaints := node.GetNodeTaints()
		for _, taint := range extraTaints {
			nodeTaints = append(nodeTaints, NewNodeTaint(taint, "", false))
		}
		result[i] = node.WithTaints(append(node.GetTaints(), extraTaints...)).WithNodeTaints(nodeTaints)
	}
	return result
}

// RemoveCordonTaints strips cordon taints (see IsCordonTaint) and clears the unschedulable flag.
// With complete metadata, it also strips non-user-facing taints, including uncategorized taints.
// Without complete metadata, it preserves non-cordon taints and leaves schedulable nodes unchanged.
//
// The submit checker uses it so that jobs targeting a node type whose
// nodes are all cordoned (via `kubectl cordon`, or otherwise reported unschedulable by the executor)
// stay queued rather than being rejected. See issue #4946. This behavior also gets applied to
// non-cordoned nodes with categorized taints.
//
// A cordoned node always has the unschedulable flag set (Kubernetes only adds
// node.kubernetes.io/unschedulable when the node is unschedulable, and the Armada taint is only
// synthesized for unschedulable nodes), so the flag alone identifies the nodes to fix.
//
// WithSchedulable only strips the Armada taint, so we strip the whole cordon taint set here
// and let WithSchedulable clear the flag.
func (f *NodeFactory) RemoveCordonTaints(nodes []*Node) []*Node {
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		if node.hasCompleteNodeTaintMetadata {
			result[i] = removeCordonTaintsWithMetadata(node)
		} else if node.IsUnschedulable() {
			result[i] = removeLegacyCordonTaints(node)
		} else {
			// Not cordoned. Freshly built node, not yet inserted into a NodeDb, so reuse it as-is.
			result[i] = node
		}
	}
	return result
}

func removeLegacyCordonTaints(node *Node) *Node {
	taints := node.GetTaints()
	nonCordonTaints := make([]v1.Taint, 0, len(taints))
	for _, taint := range taints {
		if !IsCordonTaint(taint) {
			nonCordonTaints = append(nonCordonTaints, taint)
		}
	}
	return node.WithTaints(nonCordonTaints).WithSchedulable(true)
}

func removeCordonTaintsWithMetadata(node *Node) *Node {
	nonCordonTaints := make([]v1.Taint, 0, len(node.GetTaints()))
	nodeTaints := node.GetNodeTaints()
	nonCordonNodeTaints := make([]NodeTaint, 0, len(nodeTaints))
	for _, nodeTaint := range nodeTaints {
		if !IsCordonTaint(nodeTaint.taint) && nodeTaint.GetUserFacing() {
			nonCordonNodeTaints = append(nonCordonNodeTaints, nodeTaint)
			nonCordonTaints = append(nonCordonTaints, nodeTaint.taint)
		}
	}
	return node.WithTaints(nonCordonTaints).WithNodeTaints(nonCordonNodeTaints).WithSchedulable(true)
}

func (f *NodeFactory) allocateNodeIndex() uint64 {
	return f.nodeIndexCounter.Add(1)
}
