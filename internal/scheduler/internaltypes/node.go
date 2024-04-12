package internaltypes

import (
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/label"
	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type Node struct {
	// Unique id and index of this node.
	// TODO(albin): Having both id and index is redundant.
	//              Currently, the id is "cluster name" + "node name"  and index an integer assigned on node creation.
	id    string
	index uint64

	// Executor this node belongs to and node name, which must be unique per executor.
	executor   string
	name       string
	nodeTypeId uint64

	// We need to store taints and labels separately from the node type: the latter only includes
	// indexed taints and labels, but we need all of them when checking pod requirements.
	taints []v1.Taint
	labels map[string]string

	TotalResources schedulerobjects.ResourceList

	// This field is set when inserting the Node into a NodeDb.
	Keys [][]byte

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
		nodeTypeId:            nodeTypeId,
		index:                 index,
		executor:              executor,
		name:                  name,
		taints:                koTaint.DeepCopyTaints(taints),
		labels:                deepCopyLabels(labels),
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

func (node *Node) GetName() string {
	return node.name
}

func (node *Node) GetIndex() uint64 {
	return node.index
}

func (node *Node) GetExecutor() string {
	return node.executor
}

func (node *Node) GetNodeTypeId() uint64 {
	return node.nodeTypeId
}

func (node *Node) GetLabels() map[string]string {
	return deepCopyLabels(node.labels)
}

func (node *Node) GetLabelValue(key string) (string, bool) {
	val, ok := node.labels[key]
	return val, ok
}

func (node *Node) GetTaints() []v1.Taint {
	return koTaint.DeepCopyTaints(node.taints)
}

func (node *Node) FindMatchingUntoleratedTaint(tolerations ...[]v1.Toleration) (v1.Taint, bool) {
	return koTaint.FindMatchingUntoleratedTaint(node.taints, tolerations...)
}

func (node *Node) MatchNodeSelectorTerms(nodeSelector *v1.NodeSelector) (bool, error) {
	return label.MatchNodeSelectorTerms(node.labels, nodeSelector)
}

func (node *Node) GetTolerationsForTaints() []v1.Toleration {
	var tolerations []v1.Toleration
	for _, taint := range node.taints {
		tolerations = append(tolerations, v1.Toleration{Key: taint.Key, Value: taint.Value, Effect: taint.Effect})
	}
	return tolerations
}

// UnsafeCopy returns a pointer to a new value of type Node; it is unsafe because it only makes
// shallow copies of fields that are not mutated by methods of NodeDb.
func (node *Node) UnsafeCopy() *Node {
	return &Node{
		id:         node.id,
		index:      node.index,
		executor:   node.executor,
		name:       node.name,
		nodeTypeId: node.nodeTypeId,
		taints:     node.taints,
		labels:     node.labels,

		TotalResources: node.TotalResources,

		Keys: nil,

		AllocatableByPriority: armadamaps.DeepCopy(node.AllocatableByPriority),
		AllocatedByQueue:      armadamaps.DeepCopy(node.AllocatedByQueue),
		AllocatedByJobId:      armadamaps.DeepCopy(node.AllocatedByJobId),
		EvictedJobRunIds:      maps.Clone(node.EvictedJobRunIds),
	}
}

func deepCopyLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}
	return result
}
