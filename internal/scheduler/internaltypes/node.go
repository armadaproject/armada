package internaltypes

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
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

	unschedulable bool
	overAllocated bool

	// Total space on this node
	totalResources ResourceList
	// Total space allocatable by armada jobs on this node
	// allocatableResources = totalResources - sum(unallocatableResources)
	allocatableResources ResourceList

	// This field is set when inserting the Node into a NodeDb.
	Keys [][]byte

	AllocatableByPriority map[int32]ResourceList
	AllocatedByQueue      map[string]ResourceList
	AllocatedByJobId      map[string]ResourceList
	EvictedJobRunIds      map[string]bool
	cutoffByJobId         map[string]int32
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
	allocatableByPriority := map[int32]ResourceList{}
	for _, p := range allowedPriorities {
		allocatableByPriority[p] = allocatableResources
	}
	allocatableByPriority[EvictedPriority] = allocatableResources
	allocatableByPriority[CrossPoolPriority] = allocatableResources

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
	allocatableByPriority map[int32]ResourceList,
	allocatedByQueue map[string]ResourceList,
	allocatedByJobId map[string]ResourceList,
	evictedJobRunIds map[string]bool,
	keys [][]byte,
) *Node {
	reservation := util.GetReservationName(taints)
	return &Node{
		id:                    id,
		nodeType:              nodeType,
		index:                 index,
		executor:              executor,
		name:                  name,
		pool:                  pool,
		reportingNodeType:     reportingNodeType,
		taints:                koTaint.DeepCopyTaints(taints),
		reservation:           reservation,
		labels:                deepCopyLabels(labels),
		unschedulable:         unschedulable,
		totalResources:        totalResources,
		allocatableResources:  allocatableResources,
		AllocatableByPriority: maps.Clone(allocatableByPriority),
		AllocatedByQueue:      maps.Clone(allocatedByQueue),
		AllocatedByJobId:      maps.Clone(allocatedByJobId),
		EvictedJobRunIds:      evictedJobRunIds,
		cutoffByJobId:         map[string]int32{},
		Keys:                  keys,
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
	return maps.Keys(node.AllocatedByJobId)
}

// IsJobEvicted reports whether the job is currently marked as evicted from the node.
// An evicted job still owns its resources, so this is independent of HasJobAllocation.
func (node *Node) IsJobEvicted(jobId string) bool {
	_, ok := node.EvictedJobRunIds[jobId]
	return ok
}

// HasJobAllocation reports whether the job currently owns resources on the node.
func (node *Node) HasJobAllocation(jobId string) bool {
	_, ok := node.AllocatedByJobId[jobId]
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

func (node *Node) MarkResourceUnallocatable(unallocatable ResourceList) *Node {
	result := node.DeepCopyNilKeys()

	for pri, allocatable := range result.AllocatableByPriority {
		newAllocatable := allocatable.Subtract(unallocatable).FloorAtZero()
		result.AllocatableByPriority[pri] = newAllocatable
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
	result := node.DeepCopyNilKeys()
	result.unschedulable = !schedulable
	if !schedulable {
		result.taints = append(koTaint.DeepCopyTaints(result.taints), UnschedulableTaint())
	} else {
		// Remove unschedulable taint
		taints := make([]v1.Taint, 0, len(result.taints))
		unschedulableTaint := UnschedulableTaint()
		unschedulableTaintPtr := &unschedulableTaint
		for _, taint := range taints {
			if !taint.MatchTaint(unschedulableTaintPtr) {
				taints = append(taints, taint)
			}
			result.taints = koTaint.DeepCopyTaints(taints)
		}
	}
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
		unschedulable:        node.unschedulable,
		overAllocated:        node.overAllocated,
		totalResources:       node.totalResources,
		allocatableResources: node.allocatableResources,

		// keys set to nil
		Keys: nil,

		// these maps are mutable but their keys and values are immutable
		AllocatableByPriority: maps.Clone(node.AllocatableByPriority),
		AllocatedByQueue:      maps.Clone(node.AllocatedByQueue),
		AllocatedByJobId:      maps.Clone(node.AllocatedByJobId),
		EvictedJobRunIds:      maps.Clone(node.EvictedJobRunIds),
		cutoffByJobId:         maps.Clone(node.cutoffByJobId),
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
// at or below cutoff. If the job is currently evicted from this node, it is
// un-evicted and its resources are moved out of the EvictedPriority bucket;
// ownership (AllocatedByJobId/AllocatedByQueue) is left untouched in that case
// because an evicted job still owns its resources.
func (node *Node) AddJob(job SchedulableJob, cutoff int32) error {
	jobId := job.Id()
	requests := job.KubernetesResourceRequirements()

	_, isEvicted := node.EvictedJobRunIds[jobId]
	delete(node.EvictedJobRunIds, jobId)

	if !isEvicted {
		if _, ok := node.AllocatedByJobId[jobId]; ok {
			return errors.Errorf("job %s already has resources allocated on node %s", jobId, node.GetId())
		}
		node.claimForQueueAndJob(job.Queue(), jobId, requests)
	}

	allocatable := node.AllocatableByPriority
	markAllocated(allocatable, cutoff, requests)
	if isEvicted {
		markAllocatable(allocatable, EvictedPriority, requests)
	}

	if node.cutoffByJobId == nil {
		node.cutoffByJobId = make(map[string]int32)
	}
	node.cutoffByJobId[jobId] = cutoff

	return nil
}

// EvictJob marks job as evicted from the node: its resources move from the bucket
// at the cutoff it was bound at to the EvictedPriority bucket within
// AllocatableByPriority. Ownership (AllocatedByJobId/AllocatedByQueue) is
// intentionally left in place, and the stored cutoff is preserved so a later
// RemoveJob can still release correctly.
func (node *Node) EvictJob(job SchedulableJob) error {
	jobId := job.Id()
	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.GetId())
	}

	queue := job.Queue()
	if _, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.GetId())
	}

	if node.EvictedJobRunIds == nil {
		node.EvictedJobRunIds = make(map[string]bool)
	}
	if _, ok := node.EvictedJobRunIds[jobId]; ok {
		return errors.Errorf("job %s is already evicted from node %s", jobId, node.GetId())
	}
	node.EvictedJobRunIds[jobId] = true

	allocatableByPriority := node.AllocatableByPriority
	jobRequests := job.KubernetesResourceRequirements()
	markAllocatable(allocatableByPriority, node.cutoffByJobId[jobId], jobRequests)
	markAllocated(allocatableByPriority, EvictedPriority, jobRequests)

	return nil
}

// RemoveJob unbinds job from the node, releasing its ownership and returning its
// resources to AllocatableByPriority. If the job was evicted, its resources are
// released from the EvictedPriority bucket; otherwise from the bucket at the cutoff
// it was bound at. Removing a job that is not bound is a no-op.
func (node *Node) RemoveJob(job SchedulableJob) error {
	jobId := job.Id()
	requests := job.KubernetesResourceRequirements()

	_, isEvicted := node.EvictedJobRunIds[jobId]
	delete(node.EvictedJobRunIds, jobId)

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return nil
	}

	queue := job.Queue()
	if _, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.GetId())
	}
	node.releaseForQueueAndJob(queue, jobId, requests)

	allocatable := node.AllocatableByPriority
	if isEvicted {
		markAllocatable(allocatable, EvictedPriority, requests)
	} else {
		markAllocatable(allocatable, node.cutoffByJobId[jobId], requests)
	}
	delete(node.cutoffByJobId, jobId)

	return nil
}

// releaseForQueueAndJob removes job ownership of requests from the node, deleting
// the queue entry when it reaches zero.
func (node *Node) releaseForQueueAndJob(queue, jobId string, r ResourceList) {
	delete(node.AllocatedByJobId, jobId)

	allocatedToQueue := node.AllocatedByQueue[queue].Subtract(r)
	if allocatedToQueue.AllZero() {
		delete(node.AllocatedByQueue, queue)
	} else {
		node.AllocatedByQueue[queue] = allocatedToQueue
	}
}

// claimForQueueAndJob records job ownership of requests on the node, lazily
// initialising the ownership maps.
func (node *Node) claimForQueueAndJob(queue, jobId string, r ResourceList) {
	if node.AllocatedByJobId == nil {
		node.AllocatedByJobId = make(map[string]ResourceList)
	}
	node.AllocatedByJobId[jobId] = r

	if node.AllocatedByQueue == nil {
		node.AllocatedByQueue = make(map[string]ResourceList)
	}
	node.AllocatedByQueue[queue] = node.AllocatedByQueue[queue].Add(r)
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
