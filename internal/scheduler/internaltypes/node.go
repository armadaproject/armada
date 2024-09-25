package internaltypes

import (
	"math"

	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/label"
	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/pkg/errors"
)

const (
	// evictedPriority is the priority class priority resources consumed by evicted jobs are accounted for at.
	// This helps avoid scheduling new jobs onto nodes that make it impossible to re-schedule evicted jobs.
	EvictedPriority int32 = -1
	// MinPriority is the smallest possible priority class priority within the NodeDb.
	MinPriority int32 = EvictedPriority
)

// Node is a scheduler-internal representation of one Kubernetes node.
// Its private fields should be immutable! Do not change these!
type Node struct {
	// Unique id and index of this node.
	// TODO(albin): Having both id and index is redundant.
	//              Currently, the id is "cluster name" + "node name"  and index an integer assigned on node creation.
	id    string
	index uint64

	// Executor this node belongs to and node name, which must be unique per executor.
	executor string
	name     string
	pool     string
	nodeType *NodeType

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

func FromSchedulerObjectsNode(node *schedulerobjects.Node,
	nodeIndex uint64,
	indexedTaints map[string]bool,
	indexedNodeLabels map[string]bool,
	resourceListFactory *ResourceListFactory,
) (*Node, error) {
	taints := node.GetTaints()
	if node.Unschedulable {
		taints = append(koTaint.DeepCopyTaints(taints), UnschedulableTaint())
	}

	labels := maps.Clone(node.GetLabels())
	if labels == nil {
		labels = map[string]string{}
	}
	labels[configuration.NodeIdLabel] = node.Id

	totalResources := node.TotalResources

	nodeType := NewNodeType(
		taints,
		labels,
		indexedTaints,
		indexedNodeLabels,
	)

	allocatableByPriority := map[int32]ResourceList{}
	minimumPriority := int32(math.MaxInt32)
	for p, rl := range node.AllocatableByPriorityAndResource {
		if p < minimumPriority {
			minimumPriority = p
		}
		allocatableByPriority[p] = resourceListFactory.FromNodeProto(rl.Resources)
	}
	if minimumPriority < 0 {
		return nil, errors.Errorf("found negative priority %d on node %s; negative priorities are reserved for internal use", minimumPriority, node.Id)
	}
	allocatableByPriority[EvictedPriority] = allocatableByPriority[minimumPriority]

	return CreateNode(
		node.Id,
		nodeType,
		nodeIndex,
		node.Executor,
		node.Name,
		node.Pool,
		taints,
		labels,
		resourceListFactory.FromNodeProto(totalResources.Resources),
		allocatableByPriority,
		map[string]ResourceList{},
		map[string]ResourceList{},
		map[string]bool{},
		nil), nil
}

func CreateNode(
	id string,
	nodeType *NodeType,
	index uint64,
	executor string,
	name string,
	pool string,
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
		nodeType:              nodeType,
		index:                 index,
		executor:              executor,
		name:                  name,
		pool:                  pool,
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

func (node *Node) GetPool() string {
	return node.pool
}

func (node *Node) GetIndex() uint64 {
	return node.index
}

func (node *Node) GetExecutor() string {
	return node.executor
}

func (node *Node) GetNodeTypeId() uint64 {
	return node.nodeType.GetId()
}

func (node *Node) GetNodeType() *NodeType {
	return node.nodeType
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
		pool:           node.pool,
		nodeType:       node.nodeType,
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
