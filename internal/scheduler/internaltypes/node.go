package internaltypes

import (
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type Node struct {
	// Unique id and index of this node.
	// TODO(albin): Having both id and index is redundant.
	//              Currently, the id is "cluster name" + "node name"  and index an integer assigned on node creation.
	id    string
	Index uint64

	// Executor this node belongs to and node name, which must be unique per executor.
	Executor string
	Name     string

	// We need to store taints and labels separately from the node type: the latter only includes
	// indexed taints and labels, but we need all of them when checking pod requirements.
	Taints []v1.Taint
	Labels map[string]string

	TotalResources schedulerobjects.ResourceList

	// This field is set when inserting the Node into a NodeDb.
	Keys [][]byte

	NodeTypeId uint64

	AllocatableByPriority schedulerobjects.AllocatableByPriorityAndResourceType
	AllocatedByQueue      map[string]schedulerobjects.ResourceList
	AllocatedByJobId      map[string]schedulerobjects.ResourceList
	EvictedJobRunIds      map[string]bool
}

func CreateNode(
	id string,
	nodeTypeId uint64,
	index uint64,
	executor string,
	name string,
	taints []v1.Taint,
	labels map[string]string,
	totalResources schedulerobjects.ResourceList,
	allocatableByPriority schedulerobjects.AllocatableByPriorityAndResourceType,
	allocatedByQueue map[string]schedulerobjects.ResourceList,
	allocatedByJobId map[string]schedulerobjects.ResourceList,
	evictedJobRunIds map[string]bool,
	keys [][]byte,
) *Node {
	return &Node{
		id:                    id,
		NodeTypeId:            nodeTypeId,
		Index:                 index,
		Executor:              executor,
		Name:                  name,
		Taints:                taints,
		Labels:                labels,
		TotalResources:        totalResources,
		AllocatableByPriority: allocatableByPriority,
		AllocatedByQueue:      allocatedByQueue,
		AllocatedByJobId:      allocatedByJobId,
		EvictedJobRunIds:      evictedJobRunIds,
		Keys:                  keys,
	}
}

func (node *Node) GetId() string {
	return node.id
}

// UnsafeCopy returns a pointer to a new value of type Node; it is unsafe because it only makes
// shallow copies of fields that are not mutated by methods of NodeDb.
func (node *Node) UnsafeCopy() *Node {
	return &Node{
		id:    node.id,
		Index: node.Index,

		Executor: node.Executor,
		Name:     node.Name,

		Taints: node.Taints,
		Labels: node.Labels,

		TotalResources: node.TotalResources,

		Keys: nil,

		NodeTypeId: node.NodeTypeId,

		AllocatableByPriority: armadamaps.DeepCopy(node.AllocatableByPriority),
		AllocatedByQueue:      armadamaps.DeepCopy(node.AllocatedByQueue),
		AllocatedByJobId:      armadamaps.DeepCopy(node.AllocatedByJobId),
		EvictedJobRunIds:      maps.Clone(node.EvictedJobRunIds),
	}
}
