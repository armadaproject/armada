package nodedb

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	// evictedPriority is the priority class priority resources consumed by evicted jobs are accounted for at.
	// This helps avoid scheduling new jobs onto nodes that make it impossible to re-schedule evicted jobs.
	evictedPriority int32 = -1
	// MinPriority is the smallest possible priority class priority within the NodeDb.
	MinPriority int32 = evictedPriority
)

var empty struct{}

type Node struct {
	// Unique id and index of this node.
	// TODO(albin): Having both id and index is redundant.
	//              Currently, the id is "cluster name" + "node name"  and index an integer assigned on node creation.
	Id    string
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

// UnsafeCopy returns a pointer to a new value of type Node; it is unsafe because it only makes
// shallow copies of fields that are not mutated by methods of NodeDb.
func (node *Node) UnsafeCopy() *Node {
	return &Node{
		Id:    node.Id,
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

func (nodeDb *NodeDb) create(node *schedulerobjects.Node) (*Node, error) {
	taints := node.GetTaints()
	if node.Unschedulable {
		taints = append(slices.Clone(taints), UnschedulableTaint())
	}

	labels := maps.Clone(node.GetLabels())
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[schedulerconfig.NodeIdLabel] = node.Id

	totalResources := node.TotalResources

	nodeType := schedulerobjects.NewNodeType(
		taints,
		labels,
		nodeDb.indexedTaints,
		nodeDb.indexedNodeLabels,
	)

	allocatableByPriority := schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).DeepCopy()
	minimumPriority := int32(math.MaxInt32)
	for p := range allocatableByPriority {
		if p < minimumPriority {
			minimumPriority = p
		}
	}
	if minimumPriority < 0 {
		return nil, errors.Errorf("found negative priority %d on node %s; negative priorities are reserved for internal use", minimumPriority, node.Id)
	}
	allocatableByPriority[evictedPriority] = allocatableByPriority[minimumPriority].DeepCopy()

	allocatedByQueue := node.AllocatedByQueue
	if allocatedByQueue == nil {
		allocatedByQueue = make(map[string]schedulerobjects.ResourceList)
	}

	allocatedByJobId := node.AllocatedByJobId
	if allocatedByJobId == nil {
		allocatedByJobId = make(map[string]schedulerobjects.ResourceList)
	}

	evictedJobRunIds := node.EvictedJobRunIds
	if evictedJobRunIds == nil {
		evictedJobRunIds = make(map[string]bool)
	}

	nodeDb.mu.Lock()
	for key := range nodeDb.indexedNodeLabels {
		if value, ok := labels[key]; ok {
			nodeDb.indexedNodeLabelValues[key][value] = empty
		}
	}
	index := uint64(nodeDb.numNodes)
	nodeDb.numNodes++
	nodeDb.numNodesByNodeType[nodeType.Id]++
	nodeDb.totalResources.Add(totalResources)
	nodeDb.nodeTypes[nodeType.Id] = nodeType
	nodeDb.mu.Unlock()

	return &Node{
		Id:    node.Id,
		Index: index,

		Executor: node.Executor,
		Name:     node.Name,

		Taints: taints,
		Labels: labels,

		TotalResources: totalResources,

		Keys: nil,

		NodeTypeId: nodeType.Id,

		AllocatableByPriority: allocatableByPriority,
		AllocatedByQueue:      allocatedByQueue,
		AllocatedByJobId:      allocatedByJobId,
		EvictedJobRunIds:      evictedJobRunIds,
	}, nil
}

func (nodeDb *NodeDb) CreateAndInsertWithApiJobsWithTxn(txn *memdb.Txn, jobs []*api.Job, node *schedulerobjects.Node) error {
	entry, err := nodeDb.create(node)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		priority, ok := job.GetScheduledAtPriority()
		if !ok {
			priorityClass := interfaces.PriorityClassFromLegacySchedulerJob(nodeDb.priorityClasses, nodeDb.defaultPriorityClass, job)
			priority = priorityClass.Priority
		}
		if err := nodeDb.bindJobToNodeInPlace(entry, job, priority); err != nil {
			return err
		}
	}
	if err := nodeDb.UpsertWithTxn(txn, entry); err != nil {
		return err
	}
	return nil
}

func (nodeDb *NodeDb) CreateAndInsertWithJobDbJobsWithTxn(txn *memdb.Txn, jobs []*jobdb.Job, node *schedulerobjects.Node) error {
	entry, err := nodeDb.create(node)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		priority, ok := job.GetScheduledAtPriority()
		if !ok {
			priorityClass := interfaces.PriorityClassFromLegacySchedulerJob(nodeDb.priorityClasses, nodeDb.defaultPriorityClass, job)
			priority = priorityClass.Priority
		}
		if err := nodeDb.bindJobToNodeInPlace(entry, job, priority); err != nil {
			return err
		}
	}
	if err := nodeDb.UpsertWithTxn(txn, entry); err != nil {
		return err
	}
	return nil
}

// EvictedJobSchedulingContext represents an evicted job.
// NodeDb may track these to ensure preemptions are fair.
type EvictedJobSchedulingContext struct {
	// Id of the evicted job.
	JobId string
	// Each evicted job is assigned a unique integer indicating the order in which it is re-scheduled.
	// I.e., index establishes a global order among all evicted jobs.
	//
	// When choosing on which node to schedule a job that would prevent re-scheduling evicted jobs,
	// nodeDb choses the node that would prevent re-scheduling jobs with as a large an index as possible.
	Index                int
	JobSchedulingContext *schedulercontext.JobSchedulingContext
}

// NodeDb is the scheduler-internal system used to efficiently find nodes on which a pod could be scheduled.
type NodeDb struct {
	// In-memory database storing *Node.
	db *memdb.MemDB
	// Once a node has been found on which a pod can be scheduled,
	// the NodeDb will consider up to the next maxExtraNodesToConsider nodes.
	// The NodeDb selects the node with the best score out of the considered nodes.
	// In particular, the score expresses whether preemption is necessary to schedule a pod.
	// Hence, a larger maxExtraNodesToConsider would reduce the expected number of preemptions.
	//
	// TODO: Currently gives no benefit. Since all nodes are given the same score.
	maxExtraNodesToConsider uint
	// Allowed priority classes.
	// Because the number of database indices scales linearly with the number of distinct priorities,
	// the efficiency of the NodeDb relies on the number of distinct priorities being small.
	priorityClasses map[string]types.PriorityClass
	// defaultPriorityClass is the name of the default priority class; it is
	// used for jobs that specify a priority class that does not appear in
	// priorityClasses, for example because it was deleted.
	defaultPriorityClass string
	// Priority class priorities and NodeDb-internal priority, in increasing order.
	nodeDbPriorities []int32
	// Resources, e.g., "cpu", "memory", and "nvidia.com/gpu",
	// for which indexes are created to enable efficient lookup.
	indexedResources []string
	// Like indexedResources, but stored as a map for efficient lookup.
	indexedResourcesSet map[string]interface{}
	// The resolution with which indexed resources are tracked. In the same order as indexedResources.
	// For example, if indexedResources = []string{"cpu"} and indexedResourceResolutionMillis = []int64{1000},
	// then nodes with, e.g., 2000, 2100, and 2900 mCPU allocatable are all registered as having 2000 mCPU allocatable.
	//
	// Lower resolution makes scheduling faster, but may lead to jobs incorrectly being considered unschedulable.
	indexedResourceResolutionMillis []int64
	// Map from priority class priority to the database index tracking allocatable resources at that priority.
	indexNameByPriority map[int32]string
	// Map from priority class priority to the index of node.keys corresponding to that priority.
	keyIndexByPriority map[int32]int
	// Taint keys that to create indexes for.
	// Should include taints frequently used for scheduling.
	// Since the NodeDb can efficiently sort out nodes with taints not tolerated
	// by a pod when looking for a node a pod can be scheduled on.
	//
	// If not set, all taints are indexed.
	indexedTaints map[string]interface{}
	// Node labels to create indexes for.
	// Should include node labels frequently used for scheduling.
	// Since the NodeDb can efficiently sort out nodes for which these labels
	// do not match pod node selectors when looking for a node a pod can be scheduled on.
	//
	// If not set, no labels are indexed.
	indexedNodeLabels map[string]interface{}

	// Mutex for the remaining fields of this struct, which are mutated after initialization.
	mu sync.Mutex

	// Map from indexed label names to the set of values that label takes across all nodes in the NodeDb.
	indexedNodeLabelValues map[string]map[string]struct{}
	// Total number of nodes in the db.
	numNodes int
	// Number of nodes in the db by node type.
	numNodesByNodeType map[uint64]int
	// Total amount of resources, e.g., "cpu", "memory", "gpu", across all nodes in the db.
	totalResources schedulerobjects.ResourceList
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	nodeTypes map[uint64]*schedulerobjects.NodeType

	wellKnownNodeTypes map[string]*configuration.WellKnownNodeType

	// Map from podRequirementsNotMetReason Sum64() to the string representation of that reason.
	// Used to avoid allocs.
	podRequirementsNotMetReasonStringCache map[uint64]string

	// Map from job ID to the priority class priority at which the job was scheduled.
	//
	// As of 30/11/2023, we never remove anything from this map: entries need to
	// persist for the entire scheduling round (e.g., a job may be scheduled,
	// then evicted, and then scheduled again during a single scheduling round;
	// this means that the "evict job from node" method is not allowed to
	// delete entries from scheduledAtPriorityByJobId in general) and every
	// scheduling round uses a fresh NodeDb.
	scheduledAtPriorityByJobId map[string]int32
}

func NewNodeDb(
	priorityClasses map[string]types.PriorityClass,
	maxExtraNodesToConsider uint,
	indexedResources []configuration.IndexedResource,
	indexedTaints []string,
	indexedNodeLabels []string,
	wellKnownNodeTypes []configuration.WellKnownNodeType,
) (*NodeDb, error) {
	nodeDbPriorities := []int32{evictedPriority}
	nodeDbPriorities = append(nodeDbPriorities, types.AllowedPriorities(priorityClasses)...)

	indexedResourceNames := util.Map(indexedResources, func(v configuration.IndexedResource) string { return v.Name })
	schema, indexNameByPriority, keyIndexByPriority := nodeDbSchema(nodeDbPriorities, indexedResourceNames)
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(priorityClasses) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "priorityClasses",
			Value:   priorityClasses,
			Message: "there must be at least one supported priority class",
		})
	}
	priorityClasses = maps.Clone(priorityClasses)
	if len(indexedResources) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "indexedResources",
			Value:   indexedResources,
			Message: "there must be at least one indexed resource",
		})
	}
	indexedNodeLabelValues := make(map[string]map[string]struct{}, len(indexedNodeLabels))
	for _, key := range indexedNodeLabels {
		indexedNodeLabelValues[key] = make(map[string]struct{})
	}
	mapFromSlice := func(vs []string) map[string]interface{} {
		rv := make(map[string]interface{})
		for _, v := range vs {
			rv[v] = true
		}
		return rv
	}
	nodeDb := NodeDb{
		priorityClasses:         priorityClasses,
		nodeDbPriorities:        nodeDbPriorities,
		maxExtraNodesToConsider: maxExtraNodesToConsider,
		indexedResources:        indexedResourceNames,
		indexedResourcesSet:     mapFromSlice(indexedResourceNames),
		indexedResourceResolutionMillis: util.Map(
			indexedResources,
			func(v configuration.IndexedResource) int64 { return v.Resolution.MilliValue() },
		),
		indexNameByPriority:    indexNameByPriority,
		keyIndexByPriority:     keyIndexByPriority,
		indexedTaints:          mapFromSlice(indexedTaints),
		indexedNodeLabels:      mapFromSlice(indexedNodeLabels),
		indexedNodeLabelValues: indexedNodeLabelValues,
		nodeTypes:              make(map[uint64]*schedulerobjects.NodeType),
		wellKnownNodeTypes:     make(map[string]*configuration.WellKnownNodeType),
		numNodesByNodeType:     make(map[uint64]int),
		totalResources:         schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)},
		db:                     db,
		// Set the initial capacity (somewhat arbitrarily) to 128 reasons.
		podRequirementsNotMetReasonStringCache: make(map[uint64]string, 128),

		scheduledAtPriorityByJobId: make(map[string]int32),
	}

	for _, wellKnownNodeType := range wellKnownNodeTypes {
		wellKnownNodeType := wellKnownNodeType
		nodeDb.wellKnownNodeTypes[wellKnownNodeType.Name] = &wellKnownNodeType
	}

	return &nodeDb, nil
}

// Reset clears out data specific to one scheduling round to prepare for a new scheduling round.
// Only necessary when nodeDb.enableNewPreemptionStrategy is true.
func (nodeDb *NodeDb) Reset() error {
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	it, err := txn.LowerBound("evictedJobs", "id", "")
	if err != nil {
		return errors.WithStack(err)
	}
	for obj := it.Next(); obj != nil; obj = it.Next() {
		if err := txn.Delete("evictedJobs", obj); err != nil {
			return errors.WithStack(err)
		}
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) GetScheduledAtPriority(jobId string) (int32, bool) {
	priority, ok := nodeDb.scheduledAtPriorityByJobId[jobId]
	return priority, ok
}

func (nodeDb *NodeDb) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Priorities:\t%v\n", nodeDb.nodeDbPriorities)
	fmt.Fprintf(w, "Indexed resources:\t%v\n", nodeDb.indexedResources)
	fmt.Fprintf(w, "Indexed taints:\t%v\n", maps.Keys(nodeDb.indexedTaints))
	fmt.Fprintf(w, "Indexed node labels:\t%v\n", maps.Keys(nodeDb.indexedNodeLabels))
	if len(nodeDb.nodeTypes) == 0 {
		fmt.Fprint(w, "Node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Node types:\n")
		for _, nodeType := range nodeDb.nodeTypes {
			fmt.Fprintf(w, "  %d\n", nodeType.Id)
		}
	}
	w.Flush()
	return sb.String()
}

// IndexedNodeLabelValues returns the set of possible values for a given indexed label across all nodes in the NodeDb.
func (nodeDb *NodeDb) IndexedNodeLabelValues(label string) (map[string]struct{}, bool) {
	values, ok := nodeDb.indexedNodeLabelValues[label]
	return values, ok
}

func (nodeDb *NodeDb) NumNodes() int {
	nodeDb.mu.Lock()
	defer nodeDb.mu.Unlock()
	return int(nodeDb.numNodes)
}

func (nodeDb *NodeDb) TotalResources() schedulerobjects.ResourceList {
	nodeDb.mu.Lock()
	defer nodeDb.mu.Unlock()
	return nodeDb.totalResources.DeepCopy()
}

func (nodeDb *NodeDb) Txn(write bool) *memdb.Txn {
	return nodeDb.db.Txn(write)
}

// GetNode returns a node in the db with given id.
func (nodeDb *NodeDb) GetNode(id string) (*Node, error) {
	return nodeDb.GetNodeWithTxn(nodeDb.Txn(false), id)
}

// GetNodeWithTxn returns a node in the db with given id,
// within the provided transactions.
func (nodeDb *NodeDb) GetNodeWithTxn(txn *memdb.Txn, id string) (*Node, error) {
	it, err := txn.Get("nodes", "id", id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	obj := it.Next()
	if obj == nil {
		return nil, nil
	}
	return obj.(*Node), nil
}

// NodeJobDiff compares two snapshots of the NodeDb memdb and returns
// - a map from job ids of all preempted jobs to the node they used to be on
// - a map from job ids of all scheduled jobs to the node they were scheduled on
// that happened between the two snapshots.
func NodeJobDiff(txnA, txnB *memdb.Txn) (map[string]*Node, map[string]*Node, error) {
	preempted := make(map[string]*Node)
	scheduled := make(map[string]*Node)
	nodePairIterator, err := NewNodePairIterator(txnA, txnB)
	if err != nil {
		return nil, nil, err
	}
	for item := nodePairIterator.NextItem(); item != nil; item = nodePairIterator.NextItem() {
		if item.NodeA != nil && item.NodeB == nil {
			// NodeA was removed. All jobs on NodeA are preempted.
			for jobId := range item.NodeA.AllocatedByJobId {
				preempted[jobId] = item.NodeA
			}
		} else if item.NodeA == nil && item.NodeB != nil {
			// NodeB was added. All jobs on NodeB are scheduled.
			for jobId := range item.NodeB.AllocatedByJobId {
				scheduled[jobId] = item.NodeB
			}
		} else if item.NodeA != nil && item.NodeB != nil {
			// NodeA is the same as NodeB.
			// Jobs on NodeA that are not on NodeB are preempted.
			// Jobs on NodeB that are not on NodeA are scheduled.
			for jobId := range item.NodeA.AllocatedByJobId {
				if _, ok := item.NodeB.AllocatedByJobId[jobId]; !ok {
					preempted[jobId] = item.NodeA
				}
			}
			for jobId := range item.NodeB.AllocatedByJobId {
				if _, ok := item.NodeA.AllocatedByJobId[jobId]; !ok {
					scheduled[jobId] = item.NodeB
				}
			}
		}
	}
	return preempted, scheduled, nil
}

func (nodeDb *NodeDb) ScheduleManyWithTxn(txn *memdb.Txn, gctx *schedulercontext.GangSchedulingContext) (bool, error) {
	// Attempt to schedule pods one by one in a transaction.
	numScheduled := 0
	for _, jctx := range gctx.JobSchedulingContexts {
		// In general, we may attempt to schedule a gang multiple times (in
		// order to find the best fit for this gang); clear out any remnants of
		// previous attempts.
		jctx.UnschedulableReason = ""
		jctx.ShouldFail = false

		node, err := nodeDb.SelectNodeForJobWithTxn(txn, jctx)
		if err != nil {
			return false, err
		}

		if node == nil {
			// Indicates that when the min cardinality is met, we should fail this job back to the client.
			jctx.ShouldFail = true
			continue
		}

		// If we found a node for this pod, bind it and continue to the next pod.
		if node, err := nodeDb.bindJobToNode(node, jctx.Job, jctx.PodSchedulingContext.ScheduledAtPriority); err != nil {
			return false, err
		} else {
			if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
				return false, err
			}
		}

		// Once a job is scheduled, it should no longer be considered for preemption.
		if err := deleteEvictedJobSchedulingContextIfExistsWithTxn(txn, jctx.JobId); err != nil {
			return false, err
		}

		numScheduled++
	}
	if numScheduled < gctx.GangInfo.MinimumCardinality {
		return false, nil
	}
	return true, nil
}

func deleteEvictedJobSchedulingContextIfExistsWithTxn(txn *memdb.Txn, jobId string) error {
	if err := txn.Delete("evictedJobs", &EvictedJobSchedulingContext{JobId: jobId}); err == memdb.ErrNotFound {
		return nil
	} else if err != nil {
		return errors.WithStack(err)
	} else {
		return nil
	}
}

// SelectNodeForJobWithTxn selects a node on which the job can be scheduled.
func (nodeDb *NodeDb) SelectNodeForJobWithTxn(txn *memdb.Txn, jctx *schedulercontext.JobSchedulingContext) (*Node, error) {
	req := jctx.PodRequirements
	priorityClass := interfaces.PriorityClassFromLegacySchedulerJob(nodeDb.priorityClasses, nodeDb.defaultPriorityClass, jctx.Job)

	// If the job has already been scheduled, get the priority at which it was scheduled.
	// Otherwise, get the original priority the job was submitted with.
	priority, ok := nodeDb.GetScheduledAtPriority(jctx.JobId)
	if !ok {
		priority = req.Priority
	}
	pctx := &schedulercontext.PodSchedulingContext{
		Created:                  time.Now(),
		ScheduledAtPriority:      priority,
		PreemptedAtPriority:      MinPriority,
		NumNodes:                 nodeDb.numNodes,
		NumExcludedNodesByReason: make(map[string]int),
	}
	jctx.PodSchedulingContext = pctx

	// For pods that failed to schedule, add an exclusion reason for implicitly excluded nodes.
	defer func() {
		if pctx.NodeId != "" {
			return
		}
		numExplicitlyExcludedNodes := 0
		for _, count := range pctx.NumExcludedNodesByReason {
			numExplicitlyExcludedNodes += count
		}
		numImplicitlyExcludedNodes := pctx.NumNodes - numExplicitlyExcludedNodes
		if numImplicitlyExcludedNodes > 0 {
			pctx.NumExcludedNodesByReason[PodRequirementsNotMetReasonInsufficientResources] += numImplicitlyExcludedNodes
		}
	}()

	// If the nodeIdLabel selector is set, consider only that node.
	if nodeId, ok := jctx.GetNodeSelector(schedulerconfig.NodeIdLabel); ok {
		if it, err := txn.Get("nodes", "id", nodeId); err != nil {
			return nil, errors.WithStack(err)
		} else {
			if node, err := nodeDb.selectNodeForPodWithItAtPriority(it, jctx, priority, true); err != nil {
				return nil, err
			} else {
				return node, nil
			}
		}
	}

	node, err := nodeDb.selectNodeForJobWithTxnAtPriority(txn, jctx)
	if err != nil {
		return nil, err
	}
	if node != nil {
		return node, nil
	}

	for _, awayNodeType := range priorityClass.AwayNodeTypes {
		node, err := nodeDb.selectNodeForJobWithTxnAndAwayNodeType(txn, jctx, awayNodeType)
		if err != nil {
			return nil, err
		}
		if node != nil {
			pctx.WellKnownNodeTypeName = awayNodeType.WellKnownNodeTypeName
			return node, nil
		}
	}

	return nil, nil
}

func (nodeDb *NodeDb) selectNodeForJobWithTxnAndAwayNodeType(
	txn *memdb.Txn,
	jctx *schedulercontext.JobSchedulingContext,
	awayNodeType types.AwayNodeType,
) (node *Node, err error) {
	// Save the number of additional tolerations that the job originally had; we
	// use this value to restore the slice of additional toleration at the end
	// of each loop iteration.
	numAdditionalTolerations := len(jctx.AdditionalTolerations)
	defer func() {
		// If we successfully scheduled the job on a node of this away node
		// type, keep the additional tolerations; the scheduler will add them to
		// the pod requirements overlay for the resulting run.
		if node != nil {
			return
		}
		jctx.AdditionalTolerations = jctx.AdditionalTolerations[:numAdditionalTolerations]
	}()

	wellKnownNodeType, ok := nodeDb.wellKnownNodeTypes[awayNodeType.WellKnownNodeTypeName]
	if !ok {
		return nil, fmt.Errorf("unknown well-known node type %s; must be in %v", awayNodeType.WellKnownNodeTypeName, nodeDb.wellKnownNodeTypes)
	}

	for _, taint := range wellKnownNodeType.Taints {
		jctx.AdditionalTolerations = append(jctx.AdditionalTolerations, v1.Toleration{Key: taint.Key, Value: taint.Value, Effect: taint.Effect})
	}

	jctx.PodSchedulingContext.ScheduledAtPriority = awayNodeType.Priority
	node, err = nodeDb.selectNodeForJobWithTxnAtPriority(txn, jctx)
	return
}

func (nodeDb *NodeDb) selectNodeForJobWithTxnAtPriority(
	txn *memdb.Txn,
	jctx *schedulercontext.JobSchedulingContext,
) (*Node, error) {
	pctx := jctx.PodSchedulingContext

	matchingNodeTypeIds, numExcludedNodesByReason, err := nodeDb.NodeTypesMatchingJob(jctx)
	if err != nil {
		return nil, err
	}

	// Try scheduling at evictedPriority. If this succeeds, no preemption is necessary.
	pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)
	if node, err := nodeDb.selectNodeForPodAtPriority(txn, jctx, matchingNodeTypeIds, evictedPriority); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	// Try scheduling at the job priority. If this fails, scheduling is impossible and we return.
	// This is an optimisation to avoid looking for preemption targets for unschedulable jobs.
	pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)
	if node, err := nodeDb.selectNodeForPodAtPriority(txn, jctx, matchingNodeTypeIds, pctx.ScheduledAtPriority); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node == nil {
		return nil, nil
	}
	pctx.NodeId = ""
	pctx.PreemptedAtPriority = MinPriority

	// Schedule by preventing evicted jobs from being re-scheduled.
	// This method respect fairness by preventing from re-scheduling jobs that appear as far back in the total order as possible.
	if node, err := nodeDb.selectNodeForJobWithFairPreemption(txn, jctx); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	pctx.NodeId = ""
	pctx.PreemptedAtPriority = MinPriority

	// Schedule by kicking off jobs currently bound to a node.
	// This method does not respect fairness when choosing on which node to schedule the job.
	if node, err := nodeDb.selectNodeForJobWithUrgencyPreemption(txn, jctx, matchingNodeTypeIds); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	return nil, nil
}

func assertPodSchedulingContextNode(pctx *schedulercontext.PodSchedulingContext, node *Node) error {
	if node != nil {
		if pctx.NodeId == "" {
			return errors.New("pctx.NodeId not set")
		}
		if node.Id != pctx.NodeId {
			return errors.Errorf("pctx.NodeId %s does not match node.Id %s", pctx.NodeId, node.Id)
		}
	} else if pctx.NodeId != "" {
		return errors.New("pctx.NodeId is set, but no node was returned")
	}
	return nil
}

func (nodeDb *NodeDb) selectNodeForJobWithUrgencyPreemption(
	txn *memdb.Txn,
	jctx *schedulercontext.JobSchedulingContext,
	matchingNodeTypeIds []uint64,
) (*Node, error) {
	pctx := jctx.PodSchedulingContext
	numExcludedNodesByReason := pctx.NumExcludedNodesByReason
	for _, priority := range nodeDb.nodeDbPriorities {
		if priority == evictedPriority {
			// We already tried scheduling at evictedPriority above.
			continue
		}

		// Using pctx.ScheduledAtPriority instead of jctx.PodRequirements.Priority,
		// since the pctx.ScheduledAtPriority may differ, e.g., in case of home-away scheduling.
		if priority > pctx.ScheduledAtPriority {
			break
		}

		// Reset NumExcludedNodesByReason to avoid double-counting nodes
		// (since we may consider all nodes at each priority).
		pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)

		// Try to find a node at this priority.
		if node, err := nodeDb.selectNodeForPodAtPriority(txn, jctx, matchingNodeTypeIds, priority); err != nil {
			return nil, err
		} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
			return nil, err
		} else if node != nil {
			return node, nil
		}
	}
	return nil, nil
}

func (nodeDb *NodeDb) selectNodeForPodAtPriority(
	txn *memdb.Txn,
	jctx *schedulercontext.JobSchedulingContext,
	matchingNodeTypeIds []uint64,
	priority int32,
) (*Node, error) {
	req := jctx.PodRequirements

	indexResourceRequests := make([]resource.Quantity, len(nodeDb.indexedResources))
	for i, t := range nodeDb.indexedResources {
		indexResourceRequests[i] = req.ResourceRequirements.Requests[v1.ResourceName(t)]
	}
	indexName, ok := nodeDb.indexNameByPriority[priority]
	if !ok {
		return nil, errors.Errorf("no index for priority %d; must be in %v", priority, nodeDb.indexNameByPriority)
	}
	keyIndex, ok := nodeDb.keyIndexByPriority[priority]
	if !ok {
		return nil, errors.Errorf("no key index for priority %d; must be in %v", priority, nodeDb.keyIndexByPriority)
	}
	it, err := NewNodeTypesIterator(
		txn,
		matchingNodeTypeIds,
		indexName,
		priority,
		keyIndex,
		nodeDb.indexedResources,
		indexResourceRequests,
		nodeDb.indexedResourceResolutionMillis,
	)
	if err != nil {
		return nil, err
	}

	if node, err := nodeDb.selectNodeForPodWithItAtPriority(it, jctx, priority, false); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	return nil, nil
}

func (nodeDb *NodeDb) selectNodeForPodWithItAtPriority(
	it memdb.ResultIterator,
	jctx *schedulercontext.JobSchedulingContext,
	priority int32,
	onlyCheckDynamicRequirements bool,
) (*Node, error) {
	var selectedNode *Node
	var selectedNodeScore int
	var numExtraNodes uint
	for obj := it.Next(); obj != nil; obj = it.Next() {
		if selectedNode != nil {
			numExtraNodes++
			if numExtraNodes > nodeDb.maxExtraNodesToConsider {
				break
			}
		}

		node := obj.(*Node)
		if node == nil {
			return nil, nil
		}

		var matches bool
		var score int
		var reason PodRequirementsNotMetReason
		var err error
		if onlyCheckDynamicRequirements {
			matches, score, reason = DynamicJobRequirementsMet(node.AllocatableByPriority[priority], jctx)
		} else {
			matches, score, reason, err = JobRequirementsMet(node.Taints, node.Labels, node.TotalResources, node.AllocatableByPriority[priority], jctx)
		}
		if err != nil {
			return nil, err
		}

		if matches {
			if selectedNode == nil || score > selectedNodeScore {
				selectedNode = node
				selectedNodeScore = score
				if selectedNodeScore == SchedulableBestScore {
					break
				}
			}
		} else {
			s := nodeDb.stringFromPodRequirementsNotMetReason(reason)
			jctx.PodSchedulingContext.NumExcludedNodesByReason[s] += 1
		}
	}

	if selectedNode != nil {
		jctx.PodSchedulingContext.NodeId = selectedNode.Id
		jctx.PodSchedulingContext.PreemptedAtPriority = priority
	}
	return selectedNode, nil
}

// selectNodeForJobWithFairPreemption returns a node onto which the provided job could be scheduled, or nil if none can be found.
// Specifically, it returns the node for which scheduling would result in the most "fair" preemptions.
//
// It does this by considering all evicted jobs in the reverse order they would be scheduled in and preventing
// from being re-scheduled the jobs that would be scheduled last.
func (nodeDb *NodeDb) selectNodeForJobWithFairPreemption(txn *memdb.Txn, jctx *schedulercontext.JobSchedulingContext) (*Node, error) {
	pctx := jctx.PodSchedulingContext

	var selectedNode *Node
	nodesById := make(map[string]*Node)
	evictedJobSchedulingContextsByNodeId := make(map[string][]*EvictedJobSchedulingContext)
	it, err := txn.ReverseLowerBound("evictedJobs", "index", math.MaxInt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	maxPriority := MinPriority
	for obj := it.Next(); obj != nil && selectedNode == nil; obj = it.Next() {
		evictedJobSchedulingContext := obj.(*EvictedJobSchedulingContext)
		evictedJctx := evictedJobSchedulingContext.JobSchedulingContext
		nodeId, ok := evictedJctx.GetNodeSelector(schedulerconfig.NodeIdLabel)
		if !ok {
			return nil, errors.Errorf("evicted job %s does not have a nodeIdLabel", evictedJctx.JobId)
		}
		node, ok := nodesById[nodeId]
		if !ok {
			node, err = nodeDb.GetNodeWithTxn(txn, nodeId)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		node, err = nodeDb.UnbindJobFromNode(nodeDb.priorityClasses, evictedJctx.Job, node)
		if err != nil {
			return nil, err
		}
		nodesById[nodeId] = node
		evictedJobSchedulingContextsByNodeId[nodeId] = append(evictedJobSchedulingContextsByNodeId[nodeId], evictedJobSchedulingContext)

		priority, ok := nodeDb.GetScheduledAtPriority(evictedJctx.JobId)
		if !ok {
			priority = evictedJctx.PodRequirements.Priority
		}
		if priority > maxPriority {
			maxPriority = priority
		}
		matches, _, reason, err := JobRequirementsMet(
			node.Taints,
			node.Labels,
			node.TotalResources,
			// At this point, we've unbound the jobs running on the node.
			// Hence, we should check if the job is schedulable at evictedPriority,
			// since that indicates the job can be scheduled without causing further preemptions.
			node.AllocatableByPriority[evictedPriority],
			jctx,
		)
		if err != nil {
			return nil, err
		}
		if matches {
			selectedNode = node
		} else {
			s := nodeDb.stringFromPodRequirementsNotMetReason(reason)
			pctx.NumExcludedNodesByReason[s] += 1
		}
	}
	if selectedNode != nil {
		pctx.NodeId = selectedNode.Id
		pctx.PreemptedAtPriority = maxPriority
		for _, evictedJobSchedulingContext := range evictedJobSchedulingContextsByNodeId[selectedNode.Id] {
			if err := txn.Delete("evictedJobs", evictedJobSchedulingContext); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return selectedNode, nil
}

// bindJobToNode returns a copy of node with job bound to it.
func (nodeDb *NodeDb) bindJobToNode(node *Node, job interfaces.LegacySchedulerJob, priority int32) (*Node, error) {
	node = node.UnsafeCopy()
	if err := nodeDb.bindJobToNodeInPlace(node, job, priority); err != nil {
		return nil, err
	}
	return node, nil
}

// bindJobToNodeInPlace is like bindJobToNode, but doesn't make a copy of node.
func (nodeDb *NodeDb) bindJobToNodeInPlace(node *Node, job interfaces.LegacySchedulerJob, priority int32) error {
	jobId := job.GetId()
	requests := job.GetResourceRequirements().Requests

	_, isEvicted := node.EvictedJobRunIds[jobId]
	delete(node.EvictedJobRunIds, jobId)

	if !isEvicted {
		if node.AllocatedByJobId == nil {
			node.AllocatedByJobId = make(map[string]schedulerobjects.ResourceList)
		}
		if allocatedToJob, ok := node.AllocatedByJobId[jobId]; ok {
			return errors.Errorf("job %s already has resources allocated on node %s", jobId, node.Id)
		} else {
			allocatedToJob.AddV1ResourceList(requests)
			node.AllocatedByJobId[jobId] = allocatedToJob
		}

		if node.AllocatedByQueue == nil {
			node.AllocatedByQueue = make(map[string]schedulerobjects.ResourceList)
		}
		queue := job.GetQueue()
		allocatedToQueue := node.AllocatedByQueue[queue]
		allocatedToQueue.AddV1ResourceList(requests)
		node.AllocatedByQueue[queue] = allocatedToQueue
	}

	allocatable := node.AllocatableByPriority
	allocatable.MarkAllocatedV1ResourceList(priority, requests)
	if isEvicted {
		allocatable.MarkAllocatableV1ResourceList(evictedPriority, requests)
	}

	nodeDb.scheduledAtPriorityByJobId[jobId] = priority

	return nil
}

// EvictJobsFromNode returns a copy of node with all elements of jobs for which jobFilter returns
// true evicted from it, together with a slice containing exactly those jobs.
//
// Specifically:
//
//   - The jobs that jobFilter returns true for are marked as evicted on the node.
//   - Within AllocatableByPriorityAndResource, the resources allocated to these jobs are moved from
//     the jobs' priorities to evictedPriority; they are not subtracted from AllocatedByJobId and
//     AllocatedByQueue.
func (nodeDb *NodeDb) EvictJobsFromNode(
	priorityClasses map[string]types.PriorityClass,
	jobFilter func(interfaces.LegacySchedulerJob) bool,
	jobs []interfaces.LegacySchedulerJob,
	node *Node,
) ([]interfaces.LegacySchedulerJob, *Node, error) {
	evicted := make([]interfaces.LegacySchedulerJob, 0)
	node = node.UnsafeCopy()
	for _, job := range jobs {
		if jobFilter != nil && !jobFilter(job) {
			continue
		}
		evicted = append(evicted, job)
		if err := nodeDb.evictJobFromNodeInPlace(priorityClasses, job, node); err != nil {
			return nil, nil, err
		}
	}
	return evicted, node, nil
}

// evictJobFromNodeInPlace is the in-place operation backing EvictJobsFromNode.
func (nodeDb *NodeDb) evictJobFromNodeInPlace(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) error {
	jobId := job.GetId()
	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.Id)
	}

	queue := job.GetQueue()
	if _, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	}

	if node.EvictedJobRunIds == nil {
		node.EvictedJobRunIds = make(map[string]bool)
	}
	if _, ok := node.EvictedJobRunIds[jobId]; ok {
		return errors.Errorf("job %s is already evicted from node %s", jobId, node.Id)
	}
	node.EvictedJobRunIds[jobId] = true

	allocatable := node.AllocatableByPriority
	priority, ok := nodeDb.GetScheduledAtPriority(jobId)
	if !ok {
		return errors.Errorf("job %s not mapped to a priority", jobId)
	}
	requests := job.GetResourceRequirements().Requests
	allocatable.MarkAllocatableV1ResourceList(priority, requests)
	allocatable.MarkAllocatedV1ResourceList(evictedPriority, requests)

	return nil
}

// UnbindJobsFromNode returns a node with all elements of jobs unbound from it.
func (nodeDb *NodeDb) UnbindJobsFromNode(priorityClasses map[string]types.PriorityClass, jobs []interfaces.LegacySchedulerJob, node *Node) (*Node, error) {
	node = node.UnsafeCopy()
	for _, job := range jobs {
		if err := nodeDb.unbindJobFromNodeInPlace(priorityClasses, job, node); err != nil {
			return nil, err
		}
	}
	return node, nil
}

// UnbindJobFromNode returns a copy of node with job unbound from it.
func (nodeDb *NodeDb) UnbindJobFromNode(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) (*Node, error) {
	node = node.UnsafeCopy()
	if err := nodeDb.unbindJobFromNodeInPlace(priorityClasses, job, node); err != nil {
		return nil, err
	}
	return node, nil
}

// unbindPodFromNodeInPlace is like UnbindJobFromNode, but doesn't make a copy of node.
func (nodeDb *NodeDb) unbindJobFromNodeInPlace(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) error {
	jobId := job.GetId()
	requests := job.GetResourceRequirements().Requests

	_, isEvicted := node.EvictedJobRunIds[jobId]
	delete(node.EvictedJobRunIds, jobId)

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		// Job already unbound; nothing more to do.
		return nil
	} else {
		delete(node.AllocatedByJobId, jobId)
	}

	queue := job.GetQueue()
	if allocatedToQueue, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	} else {
		allocatedToQueue.SubV1ResourceList(requests)
		if allocatedToQueue.IsZero() {
			delete(node.AllocatedByQueue, queue)
		}
	}

	allocatable := node.AllocatableByPriority
	var priority int32
	if isEvicted {
		priority = evictedPriority
	} else {
		var ok bool
		priority, ok = nodeDb.GetScheduledAtPriority(jobId)
		if !ok {
			return errors.Errorf("job %s not mapped to a priority", jobId)
		}
	}
	allocatable.MarkAllocatableV1ResourceList(priority, requests)

	return nil
}

// NodeTypesMatchingJob returns a slice with all node types a pod could be scheduled on.
// It also returns the number of nodes excluded by reason for exclusion.
func (nodeDb *NodeDb) NodeTypesMatchingJob(jctx *schedulercontext.JobSchedulingContext) ([]uint64, map[string]int, error) {
	var matchingNodeTypeIds []uint64
	numExcludedNodesByReason := make(map[string]int)
	for _, nodeType := range nodeDb.nodeTypes {
		matches, reason := NodeTypeJobRequirementsMet(nodeType, jctx)
		if matches {
			matchingNodeTypeIds = append(matchingNodeTypeIds, nodeType.Id)
		} else if reason != nil {
			s := nodeDb.stringFromPodRequirementsNotMetReason(reason)
			numExcludedNodesByReason[s] += nodeDb.numNodesByNodeType[nodeType.Id]
		} else {
			numExcludedNodesByReason[PodRequirementsNotMetReasonUnknown] += nodeDb.numNodesByNodeType[nodeType.Id]
		}
	}
	return matchingNodeTypeIds, numExcludedNodesByReason, nil
}

func (nodeDb *NodeDb) UpsertMany(nodes []*Node) error {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	if err := nodeDb.UpsertManyWithTxn(txn, nodes); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) UpsertManyWithTxn(txn *memdb.Txn, nodes []*Node) error {
	for _, node := range nodes {
		if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
			return err
		}
	}
	return nil
}

func (nodeDb *NodeDb) Upsert(node *Node) error {
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) UpsertWithTxn(txn *memdb.Txn, node *Node) error {
	keys := make([][]byte, len(nodeDb.nodeDbPriorities))
	for i, p := range nodeDb.nodeDbPriorities {
		keys[i] = nodeDb.nodeDbKey(keys[i], node.NodeTypeId, node.AllocatableByPriority[p], node.Index)
	}
	node.Keys = keys

	if err := txn.Insert("nodes", node); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ClearAllocated zeroes out allocated resources on all nodes in the NodeDb.
func (nodeDb *NodeDb) ClearAllocated() error {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	it, err := NewNodesIterator(txn)
	if err != nil {
		return err
	}
	newNodes := make([]*Node, 0)
	for node := it.NextNode(); node != nil; node = it.NextNode() {
		node = node.UnsafeCopy()
		node.AllocatableByPriority = schedulerobjects.NewAllocatableByPriorityAndResourceType(
			nodeDb.nodeDbPriorities,
			node.TotalResources,
		)
		newNodes = append(newNodes, node)
	}
	if err := nodeDb.UpsertManyWithTxn(txn, newNodes); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) AddEvictedJobSchedulingContextWithTxn(txn *memdb.Txn, index int, jctx *schedulercontext.JobSchedulingContext) error {
	if it, err := txn.Get("evictedJobs", "id", jctx.JobId); err != nil {
		return errors.WithStack(err)
	} else if obj := it.Next(); obj != nil {
		return errors.Errorf("tried to insert evicted job %s with duplicate index %d", jctx.JobId, index)
	}
	if err := txn.Insert("evictedJobs", &EvictedJobSchedulingContext{JobId: jctx.JobId, Index: index, JobSchedulingContext: jctx}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func nodeDbSchema(priorities []int32, resources []string) (*memdb.DBSchema, map[int32]string, map[int32]int) {
	nodesTable, indexNameByPriority, keyIndexByPriority := nodesTableSchema(priorities, resources)
	evictionsTable := evictionsTableSchema()
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			nodesTable.Name:     nodesTable,
			evictionsTable.Name: evictionsTable,
		},
	}, indexNameByPriority, keyIndexByPriority
}

func nodesTableSchema(priorities []int32, resources []string) (*memdb.TableSchema, map[int32]string, map[int32]int) {
	indexes := make(map[string]*memdb.IndexSchema, len(priorities)+1)
	indexes["id"] = &memdb.IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: &memdb.StringFieldIndex{Field: "Id"},
	}
	indexNameByPriority := make(map[int32]string, len(priorities))
	keyIndexByPriority := make(map[int32]int, len(priorities))
	for i, priority := range priorities {
		name := nodeIndexName(i)
		indexNameByPriority[priority] = name
		keyIndexByPriority[priority] = i
		indexes[name] = &memdb.IndexSchema{
			Name:    name,
			Unique:  true,
			Indexer: &NodeIndex{KeyIndex: i},
		}
	}
	return &memdb.TableSchema{
		Name:    "nodes",
		Indexes: indexes,
	}, indexNameByPriority, keyIndexByPriority
}

func evictionsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "evictedJobs",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "JobId"},
			},
			"index": {
				Name:    "index",
				Unique:  true,
				Indexer: &memdb.IntFieldIndex{Field: "Index"},
			},
		},
	}
}

func nodeIndexName(keyIndex int) string {
	return fmt.Sprintf("%d", keyIndex)
}

// stringFromPodRequirementsNotMetReason returns the string representation of reason,
// using a cache to avoid allocating new strings when possible.
func (nodeDb *NodeDb) stringFromPodRequirementsNotMetReason(reason PodRequirementsNotMetReason) string {
	h := reason.Sum64()
	nodeDb.mu.Lock()
	defer nodeDb.mu.Unlock()
	if s, ok := nodeDb.podRequirementsNotMetReasonStringCache[h]; ok {
		return s
	} else {
		s := reason.String()
		nodeDb.podRequirementsNotMetReasonStringCache[h] = s
		return s
	}
}

// nodeDbKey returns the index key for a particular node.
// Allocatable resources are rounded down to the closest multiple of nodeDb.indexedResourceResolutionMillis.
// This improves efficiency by reducing the number of distinct values in the index.
func (nodeDb *NodeDb) nodeDbKey(out []byte, nodeTypeId uint64, allocatable schedulerobjects.ResourceList, nodeIndex uint64) []byte {
	return RoundedNodeIndexKeyFromResourceList(
		out,
		nodeTypeId,
		nodeDb.indexedResources,
		nodeDb.indexedResourceResolutionMillis,
		allocatable,
		nodeIndex,
	)
}
