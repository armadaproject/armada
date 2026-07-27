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
	allocatableByPriority map[int32]ResourceList,
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
		allocatableByPriority,
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
		newLabels := util.MergeMaps(node.GetLabels(), extraLabels)
		result[i] = CreateNodeAndType(node.GetId(),
			node.GetIndex(),
			node.GetExecutor(),
			node.GetName(),
			node.GetPool(),
			node.GetReportingNodeType(),
			false,
			node.GetTaints(),
			newLabels,
			f.indexedTaints,
			f.indexedNodeLabels,
			node.GetTotalResources(),
			node.GetAllocatableResources(),
			node.AllocatableByPriority,
		)
	}
	return result
}

func (f *NodeFactory) AddTaints(nodes []*Node, extraTaints []v1.Taint) []*Node {
	result := make([]*Node, len(nodes))
	for i, node := range nodes {
		result[i] = CreateNodeAndType(node.GetId(),
			node.GetIndex(),
			node.GetExecutor(),
			node.GetName(),
			node.GetPool(),
			node.GetReportingNodeType(),
			false,
			append(node.GetTaints(), extraTaints...),
			node.GetLabels(),
			f.indexedTaints,
			f.indexedNodeLabels,
			node.GetTotalResources(),
			node.GetAllocatableResources(),
			node.AllocatableByPriority,
		)
	}
	return result
}

// WithTaints returns a copy of node with its taint set replaced by taints.
// Like AddTaints/RemoveCordonTaint, it rebuilds via CreateNodeAndType so the derived
// NodeType (used for nodeDb indexing) and reservation are recomputed from the new
// taint set and stay consistent.
//
// The synthesized unschedulable/cordon taint (see IsCordonTaint) is governed solely by
// node.IsUnschedulable(), not by whether taints happens to contain it: any cordon taint
// present in taints is stripped before rebuilding, and CreateNodeAndType re-derives it from
// the preserved unschedulable flag. This keeps WithTaints idempotent (passing node.GetTaints()
// back in does not duplicate the taint) and prevents a Delete modification targeting the
// cordon taint key from inadvertently removing it while the node is still unschedulable.
func (f *NodeFactory) WithTaints(node *Node, taints []v1.Taint) *Node {
	nonCordonTaints := make([]v1.Taint, 0, len(taints))
	for _, taint := range taints {
		if !IsCordonTaint(taint) {
			nonCordonTaints = append(nonCordonTaints, taint)
		}
	}
	return CreateNodeAndType(
		node.GetId(),
		node.GetIndex(),
		node.GetExecutor(),
		node.GetName(),
		node.GetPool(),
		node.GetReportingNodeType(),
		node.IsUnschedulable(),
		nonCordonTaints,
		node.GetLabels(),
		f.indexedTaints,
		f.indexedNodeLabels,
		node.GetTotalResources(),
		node.GetAllocatableResources(),
		node.AllocatableByPriority,
	)
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
// Like AddTaints/AddLabels, this rebuilds via CreateNodeAndType so NodeType (used for nodeDb
// indexing) is recomputed from the reduced taint set and stays consistent.
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
		result[i] = CreateNodeAndType(node.GetId(),
			node.GetIndex(),
			node.GetExecutor(),
			node.GetName(),
			node.GetPool(),
			node.GetReportingNodeType(),
			false,
			nonCordonTaints,
			node.GetLabels(),
			f.indexedTaints,
			f.indexedNodeLabels,
			node.GetTotalResources(),
			node.GetAllocatableResources(),
			node.AllocatableByPriority,
		)
	}
	return result
}

func (f *NodeFactory) allocateNodeIndex() uint64 {
	return f.nodeIndexCounter.Add(1)
}
