package internaltypes

import (
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/label"
	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
)

// Node is a scheduler-internal representation of one Kubernetes node.
// Its private fields should be immmutable! Do not change these!
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

	// Total space allocatable on this node
	totalResources ResourceList

	// This field is set when inserting the Node into a NodeDb.
	Keys [][]byte

	AllocatableByPriority map[int32]ResourceList
	AllocatedByQueue      map[string]ResourceList
	AllocatedByJobId      map[string]ResourceList
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
	totalResources ResourceList,
	allocatableByPriority map[int32]ResourceList,
	allocatedByQueue map[string]ResourceList,
	allocatedByJobId map[string]ResourceList,
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
		totalResources:        totalResources,
		AllocatableByPriority: maps.Clone(allocatableByPriority),
		AllocatedByQueue:      maps.Clone(allocatedByQueue),
		AllocatedByJobId:      maps.Clone(allocatedByJobId),
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

func (node *Node) GetTotalResources() ResourceList {
	return node.totalResources
}

func (node *Node) DeepCopyNilKeys() *Node {
	return &Node{
		// private fields are immutable so a shallow copy is fine
		id:             node.id,
		index:          node.index,
		executor:       node.executor,
		name:           node.name,
		nodeTypeId:     node.nodeTypeId,
		taints:         node.taints,
		labels:         node.labels,
		totalResources: node.totalResources,

		// keys set to nil
		Keys: nil,

		// these maps are mutable but their keys and values are immutable
		AllocatableByPriority: maps.Clone(node.AllocatableByPriority),
		AllocatedByQueue:      maps.Clone(node.AllocatedByQueue),
		AllocatedByJobId:      maps.Clone(node.AllocatedByJobId),
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
