package internaltypes

import (
	"fmt"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/label"
	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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
	executor          string
	name              string
	pool              string
	nodeType          *NodeType
	reportingNodeType string

	// We need to store taints and labels separately from the node type: the latter only includes
	// indexed taints and labels, but we need all of them when checking pod requirements.
	taints []v1.Taint
	labels map[string]string

	unschedulable bool

	// Total space on this node
	totalResources ResourceList
	// Total space allocatable by armada jobs on this node
	// allocatableResources = totalResources - sum(unallocatableResources)
	allocatableResources ResourceList

	unallocatableResources map[int32]ResourceList

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
	allowedPriorities []int32,
	resourceListFactory *ResourceListFactory,
) *Node {
	totalResources := resourceListFactory.FromNodeProto(node.TotalResources.Resources)
	allocatableResources := totalResources
	unallocatableResources := make(map[int32]ResourceList, len(node.UnallocatableResources))
	for p, rl := range node.UnallocatableResources {
		resourcesAsStruct := armadamaps.MapValues(rl.Resources, func(v *resource.Quantity) resource.Quantity {
			return *v
		})
		resource := resourceListFactory.FromJobResourceListIgnoreUnknown(resourcesAsStruct)
		allocatableResources = allocatableResources.Subtract(resource)
		unallocatableResources[p] = resource
	}

	allocatableByPriority := map[int32]ResourceList{}
	for _, p := range allowedPriorities {
		allocatableByPriority[p] = allocatableResources
	}
	allocatableByPriority[EvictedPriority] = allocatableResources

	taints := make([]v1.Taint, 0, len(node.Taints))
	for _, t := range node.Taints {
		if t != nil {
			taints = append(taints, *t)
		}
	}

	return CreateNodeAndType(
		node.Id,
		nodeIndex,
		node.Executor,
		node.Name,
		node.Pool,
		node.ReportingNodeType,
		node.Unschedulable,
		taints,
		node.Labels,
		indexedTaints,
		indexedNodeLabels,
		totalResources,
		allocatableResources,
		unallocatableResources,
		allocatableByPriority,
	)
}

func CreateNodeAndType(
	id string,
	index uint64,
	executor string,
	name string,
	pool string,
	reportingNodeType string,
	unschedulable bool,
	taints []v1.Taint,
	labels map[string]string,
	indexedTaints map[string]bool,
	indexedNodeLabels map[string]bool,
	totalResources ResourceList,
	allocatableResources ResourceList,
	unallocatableResources map[int32]ResourceList,
	allocatableByPriority map[int32]ResourceList,
) *Node {
	if unschedulable {
		taints = append(koTaint.DeepCopyTaints(taints), UnschedulableTaint())
	}

	if labels == nil {
		labels = map[string]string{}
	} else {
		labels = maps.Clone(labels)
	}
	labels[configuration.NodeIdLabel] = id

	nodeType := NewNodeType(
		taints,
		labels,
		indexedTaints,
		indexedNodeLabels,
	)

	return CreateNode(
		id,
		nodeType,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		taints,
		labels,
		unschedulable,
		totalResources,
		allocatableResources,
		unallocatableResources,
		allocatableByPriority,
		map[string]ResourceList{},
		map[string]ResourceList{},
		map[string]bool{},
		nil)
}

func CreateNode(
	id string,
	nodeType *NodeType,
	index uint64,
	executor string,
	name string,
	pool string,
	reportingNodeType string,
	taints []v1.Taint,
	labels map[string]string,
	unschedulable bool,
	totalResources ResourceList,
	allocatableResources ResourceList,
	unallocatableResources map[int32]ResourceList,
	allocatableByPriority map[int32]ResourceList,
	allocatedByQueue map[string]ResourceList,
	allocatedByJobId map[string]ResourceList,
	evictedJobRunIds map[string]bool,
	keys [][]byte,
) *Node {
	return &Node{
		id:                     id,
		nodeType:               nodeType,
		index:                  index,
		executor:               executor,
		name:                   name,
		pool:                   pool,
		reportingNodeType:      reportingNodeType,
		taints:                 koTaint.DeepCopyTaints(taints),
		labels:                 deepCopyLabels(labels),
		unschedulable:          unschedulable,
		totalResources:         totalResources,
		allocatableResources:   allocatableResources,
		unallocatableResources: maps.Clone(unallocatableResources),
		AllocatableByPriority:  maps.Clone(allocatableByPriority),
		AllocatedByQueue:       maps.Clone(allocatedByQueue),
		AllocatedByJobId:       maps.Clone(allocatedByJobId),
		EvictedJobRunIds:       evictedJobRunIds,
		Keys:                   keys,
	}
}

func (node *Node) GetId() string {
	return node.id
}

func (node *Node) GetName() string {
	return node.name
}

func (node *Node) IsUnschedulable() bool {
	return node.unschedulable
}

func (node *Node) GetPool() string {
	return node.pool
}

func (node *Node) GetReportingNodeType() string {
	return node.reportingNodeType
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

func (node *Node) GetAllocatableResources() ResourceList {
	return node.allocatableResources
}

func (node *Node) GetUnallocatableResources() map[int32]ResourceList {
	return maps.Clone(node.unallocatableResources)
}

func (node *Node) DeepCopyNilKeys() *Node {
	return &Node{
		// private fields are immutable so a shallow copy is fine
		id:                     node.id,
		index:                  node.index,
		executor:               node.executor,
		name:                   node.name,
		pool:                   node.pool,
		reportingNodeType:      node.reportingNodeType,
		nodeType:               node.nodeType,
		taints:                 node.taints,
		labels:                 node.labels,
		unschedulable:          node.unschedulable,
		totalResources:         node.totalResources,
		allocatableResources:   node.allocatableResources,
		unallocatableResources: node.unallocatableResources,

		// keys set to nil
		Keys: nil,

		// these maps are mutable but their keys and values are immutable
		AllocatableByPriority: maps.Clone(node.AllocatableByPriority),
		AllocatedByQueue:      maps.Clone(node.AllocatedByQueue),
		AllocatedByJobId:      maps.Clone(node.AllocatedByJobId),
		EvictedJobRunIds:      maps.Clone(node.EvictedJobRunIds),
	}
}

func (node *Node) SummaryString() string {
	if node == nil {
		return ""
	}

	result := fmt.Sprintf("Id: %s\n", node.id)
	result += fmt.Sprintf("Index: %d\n", node.index)
	result += fmt.Sprintf("Executor: %s\n", node.executor)
	result += fmt.Sprintf("Name: %s\n", node.name)
	result += fmt.Sprintf("Pool: %s\n", node.pool)
	result += fmt.Sprintf("ReportingNodeType: %s\n", node.reportingNodeType)
	result += fmt.Sprintf("Unschedulable: %t\n", node.unschedulable)
	result += fmt.Sprintf("TotalResources: %s\n", node.totalResources.String())
	result += fmt.Sprintf("AllocatableResources: %s\n", node.allocatableResources.String())
	result += fmt.Sprintf("Labels: %v\n", node.labels)
	result += fmt.Sprintf("Taints: %v\n", node.taints)
	priorities := maps.Keys(node.unallocatableResources)
	slices.Sort(priorities)
	for _, p := range priorities {
		u := node.unallocatableResources[p]
		result += fmt.Sprintf("Unallocatable at %d: %s\n", p, u.String())
	}

	return result
}

func deepCopyLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}
	return result
}
