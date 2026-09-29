package internaltypes

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/label"
	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	// EvictedPriority is the priority class priority resources consumed by evicted jobs are accounted for at.
	// This helps avoid scheduling new jobs onto nodes that make it impossible to re-schedule evicted jobs.
	EvictedPriority int32 = -2
	// CrossPoolPriority is the priority class priority resources consumed by cross-pool ("away") jobs are
	// accounted for at when PreemptCrossPoolJobsFirst is enabled. It sits below every real priority class
	// priority (which are >= 0) so any home job can urgency-preempt cross-pool jobs first, and above
	// EvictedPriority so the oversubscribed evictor can still distinguish the two.
	CrossPoolPriority int32 = -1
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
	reservation       string
	nodeType          *NodeType
	reportingNodeType string

	// We need to store taints and labels separately from the node type: the latter only includes
	// indexed taints and labels, but we need all of them when checking pod requirements.
	taints []v1.Taint
	labels map[string]string

	// Which taint and label keys the node type indexes.
	// Held so the node can recompute its own node type whenever its taints or labels change
	indexedTaints     map[string]bool
	indexedNodeLabels map[string]bool

	unschedulable bool
	overAllocated bool

	// Total space on this node
	totalResources ResourceList
	// Total space allocatable by armada jobs on this node
	// allocatableResources = totalResources - sum(unallocatableResources)
	allocatableResources ResourceList

	// This field is set when inserting the Node into a NodeDb.
	Keys [][]byte

	// Two views of what is still allocatable, both keyed by priority. In each, the bucket at
	// priority P holds allocatableResources minus every job accounted for at a priority >= P, so a
	// job at P sees resources held by lower-priority jobs as available to it: those could be freed
	// by preempting them.
	//
	// They differ only in how they treat eviction:
	//
	//   - allocatableByPriority gives an evicted job's resources back at the priority it was bound
	//     at, and instead deducts them at EvictedPriority. This is the view normal scheduling and
	//     fair-share preemption read.
	//   - allocatableByPriorityNoEviction ignores eviction entirely, keeping the job deducted at the
	//     priority it was bound at. This is the view urgency-based preemption reads, so that the
	//     give-back above is not mistaken for capacity urgency preemption could free. Only fair-share
	//     preemption can reclaim an evicted job's resources, by holding it back from rescheduling.
	//
	// The two therefore agree whenever nothing on the node is evicted.
	allocatableByPriority           map[int32]ResourceList
	allocatableByPriorityNoEviction map[int32]ResourceList
	allocatedByJobId                map[string]ResourceList
	evictedJobRunIds                map[string]bool
	priorityByJobId                 map[string]int32
	// Priorities present in allocatableByPriority
	// Sorted ascending as HasUrgencyPreemptibleResources relies on the ordering to find the highest/lowest priority
	// This must stay sorted
	knownPriorities []int32
}

func FromSchedulerObjectsNode(node *schedulerobjects.Node,
	nodeIndex uint64,
	indexedTaints map[string]bool,
	indexedNodeLabels map[string]bool,
	allowedPriorities []int32,
	resourceListFactory *ResourceListFactory,
) *Node {
	totalResources := resourceListFactory.FromNodeProto(node.TotalResources.Resources)
	allocatableResources := resourceListFactory.FromNodeProto(node.AvailableArmadaResource().ToProtoMap())

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
		allowedPriorities,
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
	allowedPriorities []int32,
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

	return CreateNode(
		id,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		taints,
		labels,
		indexedTaints,
		indexedNodeLabels,
		unschedulable,
		totalResources,
		allocatableResources,
		allowedPriorities,
		nil)
}

func CreateNode(
	id string,
	index uint64,
	executor string,
	name string,
	pool string,
	reportingNodeType string,
	taints []v1.Taint,
	labels map[string]string,
	indexedTaints map[string]bool,
	indexedNodeLabels map[string]bool,
	unschedulable bool,
	totalResources ResourceList,
	allocatableResources ResourceList,
	allowedPriorities []int32,
	keys [][]byte,
) *Node {
	taints = koTaint.DeepCopyTaints(taints)
	labels = deepCopyLabels(labels)
	allocatableByPriority := NewAllocatableByPriorityAndResourceType(allowedPriorities, allocatableResources)
	// maps.Keys returns keys in an unspecified order; sort so that
	// HasUrgencyPreemptibleResources can read the lowest and highest priority
	// off the ends of the slice.
	knownPriorities := maps.Keys(allocatableByPriority)
	slices.Sort(knownPriorities)
	return &Node{
		id:                              id,
		nodeType:                        NewNodeType(taints, labels, indexedTaints, indexedNodeLabels),
		index:                           index,
		executor:                        executor,
		name:                            name,
		pool:                            pool,
		reportingNodeType:               reportingNodeType,
		taints:                          taints,
		reservation:                     util.GetReservationName(taints),
		labels:                          labels,
		indexedTaints:                   indexedTaints,
		indexedNodeLabels:               indexedNodeLabels,
		unschedulable:                   unschedulable,
		totalResources:                  totalResources,
		allocatableResources:            allocatableResources,
		allocatableByPriority:           allocatableByPriority,
		allocatableByPriorityNoEviction: NewAllocatableByPriorityAndResourceType(allowedPriorities, allocatableResources),
		allocatedByJobId:                map[string]ResourceList{},
		evictedJobRunIds:                map[string]bool{},
		priorityByJobId:                 map[string]int32{},
		knownPriorities:                 knownPriorities,
		Keys:                            keys,
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

func (node *Node) IsOverAllocated() bool {
	return node.overAllocated
}

func (node *Node) GetPool() string {
	return node.pool
}

func (node *Node) GetReservation() string {
	return node.reservation
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

func (node *Node) GetRunningJobIds() []string {
	return maps.Keys(node.allocatedByJobId)
}

func (node *Node) HasAllocatedJobs() bool {
	return len(node.allocatedByJobId) > 0
}

func (node *Node) AllocatedByJob() map[string]ResourceList {
	return maps.Clone(node.allocatedByJobId)
}

func (node *Node) IsJobEvicted(jobId string) bool {
	_, ok := node.evictedJobRunIds[jobId]
	return ok
}

func (node *Node) EvictedJobRunIds() map[string]bool {
	return maps.Clone(node.evictedJobRunIds)
}

func (node *Node) HasJobAllocation(jobId string) bool {
	_, ok := node.allocatedByJobId[jobId]
	return ok
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

// KnownPriorities returns the priorities this node tracks allocatable resources at,
// in ascending order. This includes EvictedPriority and CrossPoolPriority.
func (node *Node) KnownPriorities() []int32 {
	return slices.Clone(node.knownPriorities)
}

func (node *Node) AllocatableAtPriority(priority int32) ResourceList {
	return node.allocatableByPriority[priority]
}

func (node *Node) AllocatableByPriority() map[int32]ResourceList {
	return maps.Clone(node.allocatableByPriority)
}

func (node *Node) AllocatableAtPriorityNoEviction(priority int32) ResourceList {
	return node.allocatableByPriorityNoEviction[priority]
}

func (node *Node) AllocatableByPriorityNoEviction() map[int32]ResourceList {
	return maps.Clone(node.allocatableByPriorityNoEviction)
}

func (node *Node) WithNodeType(nodeType *NodeType) *Node {
	result := node.DeepCopyNilKeys()
	result.nodeType = nodeType
	return result
}

func (node *Node) WithId(id string) *Node {
	result := node.DeepCopyNilKeys()
	result.id = id
	return result
}

func (node *Node) WithIndex(index uint64) *Node {
	result := node.DeepCopyNilKeys()
	result.index = index
	return result
}

func (node *Node) WithTaints(taints []v1.Taint) *Node {
	result := node.DeepCopyNilKeys()
	result.taints = koTaint.DeepCopyTaints(taints)
	result.reservation = util.GetReservationName(result.taints)
	result.nodeType = NewNodeType(result.taints, result.labels, node.indexedTaints, node.indexedNodeLabels)
	return result
}

func (node *Node) WithLabels(labels map[string]string) *Node {
	result := node.DeepCopyNilKeys()
	result.labels = deepCopyLabels(labels)
	result.nodeType = NewNodeType(result.taints, result.labels, node.indexedTaints, node.indexedNodeLabels)
	return result
}

// WithResourcesUsedAtPriority returns a copy of node with rs deducted from every
// bucket at or below priority, preserving all other state. Unlike AddJob this
// records no job ownership, so the node cannot later release these resources.
// Only expected to be used from tests
func (node *Node) WithResourcesUsedAtPriority(priority int32, rs ResourceList) *Node {
	result := node.DeepCopyNilKeys()
	markAllocated(result.allocatableByPriority, priority, rs)
	markAllocated(result.allocatableByPriorityNoEviction, priority, rs)
	return result
}

func (node *Node) HasUrgencyPreemptibleResources() bool {
	if len(node.knownPriorities) == 0 {
		return false
	}
	lowest := node.allocatableByPriorityNoEviction[node.knownPriorities[0]]
	highest := node.allocatableByPriorityNoEviction[node.knownPriorities[len(node.knownPriorities)-1]]
	return !lowest.Equal(highest)
}

func (node *Node) MarkResourceUnallocatable(unallocatable ResourceList) *Node {
	result := node.DeepCopyNilKeys()

	for pri, allocatable := range result.allocatableByPriority {
		newAllocatable := allocatable.Subtract(unallocatable).FloorAtZero()
		result.allocatableByPriority[pri] = newAllocatable
	}
	for pri, allocatable := range result.allocatableByPriorityNoEviction {
		newAllocatable := allocatable.Subtract(unallocatable).FloorAtZero()
		result.allocatableByPriorityNoEviction[pri] = newAllocatable
	}

	result.allocatableResources = result.allocatableResources.Subtract(unallocatable).FloorAtZero()
	return result
}

func (node *Node) WithOverAllocated(overAllocated bool) *Node {
	result := node.DeepCopyNilKeys()
	result.overAllocated = overAllocated
	return result
}

func (node *Node) WithSchedulable(schedulable bool) *Node {
	if node.unschedulable == !schedulable {
		// Already in the requested state. Returning early also stops a second
		// WithSchedulable(false) from appending a duplicate unschedulable taint.
		return node
	}

	unschedulableTaint := UnschedulableTaint()
	var taints []v1.Taint
	if schedulable {
		taints = make([]v1.Taint, 0, len(node.taints))
		for _, taint := range node.taints {
			if !taint.MatchTaint(&unschedulableTaint) {
				taints = append(taints, taint)
			}
		}
	} else {
		taints = append(node.GetTaints(), unschedulableTaint)
	}

	result := node.WithTaints(taints)
	result.unschedulable = !schedulable
	return result
}

func (node *Node) DeepCopyNilKeys() *Node {
	return &Node{
		// private fields are immutable so a shallow copy is fine
		id:                   node.id,
		index:                node.index,
		executor:             node.executor,
		name:                 node.name,
		pool:                 node.pool,
		reservation:          node.reservation,
		reportingNodeType:    node.reportingNodeType,
		nodeType:             node.nodeType,
		taints:               node.taints,
		labels:               node.labels,
		indexedTaints:        node.indexedTaints,
		indexedNodeLabels:    node.indexedNodeLabels,
		unschedulable:        node.unschedulable,
		overAllocated:        node.overAllocated,
		totalResources:       node.totalResources,
		allocatableResources: node.allocatableResources,

		// keys set to nil
		Keys: nil,

		// The copy is about to be mutated in place by AddJob/EvictJob/RemoveJob, so these
		// maps must not be shared with the original
		allocatableByPriority:           maps.Clone(node.allocatableByPriority),
		allocatableByPriorityNoEviction: maps.Clone(node.allocatableByPriorityNoEviction),
		allocatedByJobId:                maps.Clone(node.allocatedByJobId),
		evictedJobRunIds:                maps.Clone(node.evictedJobRunIds),
		priorityByJobId:                 maps.Clone(node.priorityByJobId),
		knownPriorities:                 node.knownPriorities,
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
	result += fmt.Sprintf("Reservation: %s\n", node.reservation)
	result += fmt.Sprintf("ReportingNodeType: %s\n", node.reportingNodeType)
	result += fmt.Sprintf("Unschedulable: %t\n", node.unschedulable)
	result += fmt.Sprintf("OverAllocated: %t\n", node.overAllocated)
	result += fmt.Sprintf("TotalResources: %s\n", node.totalResources.String())
	result += fmt.Sprintf("AllocatableResources: %s\n", node.allocatableResources.String())
	result += fmt.Sprintf("Labels: %v\n", node.labels)
	result += fmt.Sprintf("Taints: %v\n", node.taints)
	return result
}

func deepCopyLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}
	return result
}

// SchedulableJob is the subset of a job the Node needs to account for its
// resources. *jobdb.Job satisfies this interface.
type SchedulableJob interface {
	Id() string
	Queue() string
	KubernetesResourceRequirements() ResourceList
}

// AddJob binds job to the node, deducting its resources at every priority bucket
// at or below priority.
//
// Will error if attempting to add a job that already exists on the node or
// rebinding an evicted job at a different priority
func (node *Node) AddJob(job SchedulableJob, priority int32) error {
	jobId := job.Id()
	requests := job.KubernetesResourceRequirements()

	if node.IsJobEvicted(jobId) {
		if evictedAtPriority, ok := node.priorityByJobId[jobId]; ok && evictedAtPriority != priority {
			return errors.Errorf(
				"job %s is evicted from node %s at priority %d, but was re-bound at priority %d",
				jobId, node.GetId(), evictedAtPriority, priority,
			)
		}
		if err := node.RemoveJob(job); err != nil {
			return err
		}
	}

	if _, ok := node.allocatedByJobId[jobId]; ok {
		return errors.Errorf("job %s already has resources allocated on node %s", jobId, node.GetId())
	}
	node.allocatedByJobId[jobId] = requests
	node.priorityByJobId[jobId] = priority
	markAllocated(node.allocatableByPriority, priority, requests)
	markAllocated(node.allocatableByPriorityNoEviction, priority, requests)

	return nil
}

// EvictJob marks job as evicted from the node: its resources move from the bucket
// at the priority it was bound at to the EvictedPriority bucket within
// allocatableByPriority. Ownership (allocatedByJobId) is
// intentionally left in place, and the stored priority is preserved so a later
// RemoveJob can still release correctly.
func (node *Node) EvictJob(job SchedulableJob) error {
	jobId := job.Id()
	if _, ok := node.allocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.GetId())
	}

	if node.IsJobEvicted(jobId) {
		return errors.Errorf("job %s is already evicted from node %s", jobId, node.GetId())
	}
	node.evictedJobRunIds[jobId] = true

	allocatableByPriority := node.allocatableByPriority
	jobRequests := job.KubernetesResourceRequirements()
	markAllocatable(allocatableByPriority, node.priorityByJobId[jobId], jobRequests)
	markAllocated(allocatableByPriority, EvictedPriority, jobRequests)

	return nil
}

// RemoveJob unbinds job from the node, releasing its ownership and returning its
// resources to allocatableByPriority. If the job was evicted, its resources are
// released from the EvictedPriority bucket; otherwise from the bucket at the priority
// it was bound at. Removing a job that is not bound is a no-op.
func (node *Node) RemoveJob(job SchedulableJob) error {
	jobId := job.Id()
	requests := job.KubernetesResourceRequirements()

	isEvicted := node.IsJobEvicted(jobId)
	delete(node.evictedJobRunIds, jobId)

	if _, ok := node.allocatedByJobId[jobId]; !ok {
		return nil
	}
	delete(node.allocatedByJobId, jobId)

	if isEvicted {
		markAllocatable(node.allocatableByPriority, EvictedPriority, requests)
	} else {
		markAllocatable(node.allocatableByPriority, node.priorityByJobId[jobId], requests)
	}
	markAllocatable(node.allocatableByPriorityNoEviction, node.priorityByJobId[jobId], requests)
	delete(node.priorityByJobId, jobId)

	return nil
}

func markAllocated(allocatableByPriority map[int32]ResourceList, priorityCutoff int32, rs ResourceList) {
	markAllocatable(allocatableByPriority, priorityCutoff, rs.Negate())
}

func markAllocatable(allocatableByPriority map[int32]ResourceList, priorityCutoff int32, rs ResourceList) {
	priorities := make([]int32, 0, len(allocatableByPriority))
	for priority := range allocatableByPriority {
		if priority <= priorityCutoff {
			priorities = append(priorities, priority)
		}
	}
	for _, priority := range priorities {
		allocatableByPriority[priority] = allocatableByPriority[priority].Add(rs)
	}
}
