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
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
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
	Id       string
	Name     string
	Executor string

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
		Id:       node.Id,
		Name:     node.Name,
		Executor: node.Executor,

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
	nodeDb.numNodes++
	nodeDb.numNodesByNodeType[nodeType.Id]++
	nodeDb.totalResources.Add(totalResources)
	nodeDb.nodeTypes[nodeType.Id] = nodeType
	nodeDb.mu.Unlock()

	entry := &Node{
		Id:       node.Id,
		Name:     node.Name,
		Executor: node.Executor,

		Taints: taints,
		Labels: labels,

		TotalResources: totalResources,

		Keys: nil,

		NodeTypeId: nodeType.Id,

		AllocatableByPriority: allocatableByPriority,
		AllocatedByQueue:      allocatedByQueue,
		AllocatedByJobId:      allocatedByJobId,
		EvictedJobRunIds:      evictedJobRunIds,
	}
	return entry, nil
}

func (nodeDb *NodeDb) CreateAndInsertWithApiJobsWithTxn(txn *memdb.Txn, jobs []*api.Job, node *schedulerobjects.Node) error {
	entry, err := nodeDb.create(node)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := bindJobToNodeInPlace(nodeDb.priorityClasses, job, entry); err != nil {
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
		if err := bindJobToNodeInPlace(nodeDb.priorityClasses, job, entry); err != nil {
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
	// Prioritiy class priorities in increasing order.
	priorityClassPriorities []int32
	// Job priorities supported by the NodeDb. Composed of priority class priorities and nodeDb-internal priorities.
	// In particular, if a job has priority class priority p, nodeDb tries to schedule that job at priority
	// nodeDbPriorities[0], ..., nodeDbPriorities[i],
	// for all i such that nodeDbPriorities[i] <= the priority of the job.
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
	// Map from priority class priority to the index tracking allocatable resources at that priority.
	indexNameByPriority map[int32]string
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

	// If true, use experimental preemption strategy.
	enableNewPreemptionStrategy bool
}

func NewNodeDb(
	priorityClasses map[string]types.PriorityClass,
	maxExtraNodesToConsider uint,
	indexedResources []configuration.IndexedResource,
	indexedTaints []string,
	indexedNodeLabels []string,
	wellKnownNodeTypes []configuration.WellKnownNodeType,
) (*NodeDb, error) {
	if len(indexedResources) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "indexedResources",
			Value:   indexedResources,
			Message: "there must be at least one index resource",
		})
	}
	indexedResourceNames := util.Map(indexedResources, func(v configuration.IndexedResource) string { return v.Name })
	allowedPriorities := make(map[int32]bool, len(priorityClasses))
	for _, pc := range priorityClasses {
		allowedPriorities[pc.Priority] = true
	}
	priorityClassPriorities := maps.Keys(allowedPriorities)
	slices.Sort(priorityClassPriorities)
	nodeDbPriorities := armadaslices.Concatenate([]int32{evictedPriority}, priorityClassPriorities)
	schema, indexNameByPriority := nodeDbSchema(nodeDbPriorities, indexedResourceNames)
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
		priorityClassPriorities: priorityClassPriorities,
		nodeDbPriorities:        nodeDbPriorities,
		maxExtraNodesToConsider: maxExtraNodesToConsider,
		indexedResources:        indexedResourceNames,
		indexedResourcesSet:     mapFromSlice(indexedResourceNames),
		indexedResourceResolutionMillis: util.Map(
			indexedResources,
			func(v configuration.IndexedResource) int64 { return v.Resolution.MilliValue() },
		),
		indexNameByPriority:    indexNameByPriority,
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
	}

	for _, wellKnownNodeType := range wellKnownNodeTypes {
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

func (nodeDb *NodeDb) EnableNewPreemptionStrategy() {
	nodeDb.enableNewPreemptionStrategy = true
}

func (nodeDb *NodeDb) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Priorities:\t%v\n", configuration.AllowedPriorities(nodeDb.priorityClasses))
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
	return nodeDb.numNodes
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

// ScheduleMany assigns a set of jobs to nodes.
// If N jobs can be scheduled, where N >= `GangMinCardinality`, it will return true, nil and set ShouldFail on any excess jobs.
// Otherwise, it will return false, nil.
// TODO: Pass through contexts to support timeouts.
func (nodeDb *NodeDb) ScheduleMany(jctxs []*schedulercontext.JobSchedulingContext) (bool, error) {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	ok, err := nodeDb.ScheduleManyWithTxn(txn, jctxs)
	if ok && err == nil {
		// All pods can be scheduled; commit the transaction.
		txn.Commit()
	}
	return ok, err
}

// TODO: Remove me once we re-phrase nodedb in terms of gang context (and therefore can just take this value from the gang scheduling context provided)
func gangMinCardinality(jctxs []*schedulercontext.JobSchedulingContext) int {
	if len(jctxs) > 0 {
		return jctxs[0].GangMinCardinality
	} else {
		return 1
	}
}

func (nodeDb *NodeDb) ScheduleManyWithTxn(txn *memdb.Txn, jctxs []*schedulercontext.JobSchedulingContext) (bool, error) {
	// Attempt to schedule pods one by one in a transaction.
	cumulativeScheduled := 0
	gangMinCardinality := gangMinCardinality(jctxs)

	for _, jctx := range jctxs {
		// Defensively reset `ShouldFail` (this should always be false as the state is re-constructed per cycle but just in case)
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
		if node, err := bindJobToNode(nodeDb.priorityClasses, jctx.Job, node); err != nil {
			return false, err
		} else {
			if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
				return false, err
			}
		}

		// Once a job is scheduled, it should no longer be considered for preemption.
		if nodeDb.enableNewPreemptionStrategy {
			if err := deleteEvictedJobSchedulingContextIfExistsWithTxn(txn, jctx.JobId); err != nil {
				return false, err
			}
		}

		cumulativeScheduled++
	}

	if cumulativeScheduled < gangMinCardinality {
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

	// Collect all node types that could potentially schedule the pod.
	matchingNodeTypes, numExcludedNodesByReason, err := nodeDb.NodeTypesMatchingPod(req)
	if err != nil {
		return nil, err
	}

	// Create a pctx to be returned to the caller.
	pctx := &schedulercontext.PodSchedulingContext{
		Created:           time.Now(),
		MatchingNodeTypes: matchingNodeTypes,
		NumNodes:          nodeDb.numNodes,
		// TODO: This clone looks unnecessary.
		NumExcludedNodesByReason: maps.Clone(numExcludedNodesByReason),
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
			pctx.NumExcludedNodesByReason[schedulerobjects.PodRequirementsNotMetReasonInsufficientResources] += numImplicitlyExcludedNodes
		}
	}()

	// If the targetNodeIdAnnocation is set, consider only that node.
	if nodeId, ok := req.NodeSelector[schedulerconfig.NodeIdLabel]; ok {
		if it, err := txn.Get("nodes", "id", nodeId); err != nil {
			return nil, errors.WithStack(err)
		} else {
			if node, err := nodeDb.selectNodeForPodWithIt(pctx, it, req.Priority, req, true); err != nil {
				return nil, err
			} else {
				return node, nil
			}
		}
	}

	// Try scheduling at evictedPriority. If this succeeds, no preemption is necessary.
	pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)
	if node, err := nodeDb.selectNodeForPodAtPriority(txn, pctx, evictedPriority, jctx.PodRequirements); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	// Try scheduling at the job priority. If this fails, scheduling is impossible and we return.
	// This is an optimisation to avoid looking for preemption targets for unschedulable jobs.
	pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)
	if node, err := nodeDb.selectNodeForPodAtPriority(txn, pctx, jctx.PodRequirements.Priority, jctx.PodRequirements); err != nil {
		return nil, err
	} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
		return nil, err
	} else if node == nil {
		return nil, nil
	}
	pctx.NodeId = ""
	pctx.Score = 0
	pctx.ScheduledAtPriority = 0

	// Schedule by preventing evicted jobs from being re-scheduled.
	// This method respect fairness by preventing from re-scheduling jobs that appear as far back in the total order as possible.
	if nodeDb.enableNewPreemptionStrategy {
		if node, err := nodeDb.selectNodeForJobWithFairPreemption(txn, jctx); err != nil {
			return nil, err
		} else if err := assertPodSchedulingContextNode(pctx, node); err != nil {
			return nil, err
		} else if node != nil {
			return node, nil
		}
	}
	pctx.NodeId = ""
	pctx.Score = 0
	pctx.ScheduledAtPriority = 0

	// Schedule by kicking off jobs currently bound to a node.
	// This method does not respect fairness when choosing on which node to schedule the job.
	if node, err := nodeDb.selectNodeForJobWithUrgencyPreemption(txn, jctx); err != nil {
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
) (*Node, error) {
	pctx := jctx.PodSchedulingContext
	req := jctx.PodRequirements
	numExcludedNodesByReason := pctx.NumExcludedNodesByReason
	// TODO: This doesn't need to include the evictedPriority now.
	for _, priority := range nodeDb.priorityClassPriorities {
		if priority > req.Priority {
			break
		}

		// Reset NumExcludedNodesByReason to avoid double-counting nodes
		// (since we may consider all nodes at each priority).
		pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)

		// Try to find a node at this priority.
		if node, err := nodeDb.selectNodeForPodAtPriority(txn, pctx, priority, req); err != nil {
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
	pctx *schedulercontext.PodSchedulingContext,
	priority int32,
	req *schedulerobjects.PodRequirements,
) (*Node, error) {
	nodeTypeIds := make([]uint64, len(pctx.MatchingNodeTypes))
	for i, nodeType := range pctx.MatchingNodeTypes {
		nodeTypeIds[i] = nodeType.Id
	}

	indexResourceRequests := make([]resource.Quantity, len(nodeDb.indexedResources))
	for i, t := range nodeDb.indexedResources {
		indexResourceRequests[i] = req.ResourceRequirements.Requests[v1.ResourceName(t)]
	}
	indexName, ok := nodeDb.indexNameByPriority[priority]
	if !ok {
		return nil, errors.Errorf("no index for priority %d; must be in %v", priority, nodeDb.indexNameByPriority)
	}
	it, err := NewNodeTypesIterator(
		txn,
		nodeTypeIds,
		indexName,
		priority,
		nodeDb.indexedResources,
		indexResourceRequests,
		nodeDb.indexedResourceResolutionMillis,
	)
	if err != nil {
		return nil, err
	}

	if node, err := nodeDb.selectNodeForPodWithIt(pctx, it, priority, req, false); err != nil {
		return nil, err
	} else if node != nil {
		return node, nil
	}

	return nil, nil
}

func (nodeDb *NodeDb) selectNodeForPodWithIt(
	pctx *schedulercontext.PodSchedulingContext,
	it memdb.ResultIterator,
	priority int32,
	req *schedulerobjects.PodRequirements,
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
		var reason schedulerobjects.PodRequirementsNotMetReason
		var err error
		if onlyCheckDynamicRequirements {
			matches, score, reason, err = schedulerobjects.DynamicPodRequirementsMet(node.AllocatableByPriority[priority], req)
		} else {
			matches, score, reason, err = schedulerobjects.PodRequirementsMet(node.Taints, node.Labels, node.TotalResources, node.AllocatableByPriority[priority], req)
		}
		if err != nil {
			return nil, err
		}

		if matches {
			if selectedNode == nil || score > selectedNodeScore {
				selectedNode = node
				selectedNodeScore = score
				if selectedNodeScore == schedulerobjects.SchedulableBestScore {
					break
				}
			}
		} else {
			s := nodeDb.stringFromPodRequirementsNotMetReason(reason)
			pctx.NumExcludedNodesByReason[s] += 1
		}
	}

	if selectedNode != nil {
		pctx.NodeId = selectedNode.Id
		pctx.Score = selectedNodeScore
		pctx.ScheduledAtPriority = priority
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
	for obj := it.Next(); obj != nil && selectedNode == nil; obj = it.Next() {
		evictedJobSchedulingContext := obj.(*EvictedJobSchedulingContext)
		evictedJctx := evictedJobSchedulingContext.JobSchedulingContext
		evictedReq := evictedJctx.PodRequirements

		nodeId, ok := evictedReq.NodeSelector[schedulerconfig.NodeIdLabel]
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
		node, err = UnbindJobFromNode(nodeDb.priorityClasses, evictedJctx.Job, node)
		if err != nil {
			return nil, err
		}
		nodesById[nodeId] = node
		evictedJobSchedulingContextsByNodeId[nodeId] = append(evictedJobSchedulingContextsByNodeId[nodeId], evictedJobSchedulingContext)

		matches, _, reason, err := schedulerobjects.PodRequirementsMet(
			node.Taints,
			node.Labels,
			node.TotalResources,
			node.AllocatableByPriority[evictedPriority],
			jctx.PodRequirements,
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
		pctx.ScheduledAtPriority = jctx.PodRequirements.Priority
		for _, evictedJobSchedulingContext := range evictedJobSchedulingContextsByNodeId[selectedNode.Id] {
			if err := txn.Delete("evictedJobs", evictedJobSchedulingContext); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	return selectedNode, nil
}

// bindJobToNode returns a copy of node with job bound to it.
func bindJobToNode(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) (*Node, error) {
	node = node.UnsafeCopy()
	if err := bindJobToNodeInPlace(priorityClasses, job, node); err != nil {
		return nil, err
	}
	return node, nil
}

// bindJobToNodeInPlace is like bindJobToNode, but doesn't make a copy of node.
func bindJobToNodeInPlace(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) error {
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
	priority := priorityClasses[job.GetPriorityClassName()].Priority
	allocatable.MarkAllocatedV1ResourceList(priority, requests)
	if isEvicted {
		allocatable.MarkAllocatableV1ResourceList(evictedPriority, requests)
	}

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
func EvictJobsFromNode(
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
		if err := evictJobFromNodeInPlace(priorityClasses, job, node); err != nil {
			return nil, nil, err
		}
	}
	return evicted, node, nil
}

// evictJobFromNodeInPlace is the in-place operation backing EvictJobsFromNode.
func evictJobFromNodeInPlace(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) error {
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
	priority := priorityClasses[job.GetPriorityClassName()].Priority
	requests := job.GetResourceRequirements().Requests
	allocatable.MarkAllocatableV1ResourceList(priority, requests)
	allocatable.MarkAllocatedV1ResourceList(evictedPriority, requests)

	return nil
}

// UnbindJobsFromNode returns a node with all elements of jobs unbound from it.
func UnbindJobsFromNode(priorityClasses map[string]types.PriorityClass, jobs []interfaces.LegacySchedulerJob, node *Node) (*Node, error) {
	node = node.UnsafeCopy()
	for _, job := range jobs {
		if err := unbindJobFromNodeInPlace(priorityClasses, job, node); err != nil {
			return nil, err
		}
	}
	return node, nil
}

// UnbindJobFromNode returns a copy of node with job unbound from it.
func UnbindJobFromNode(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) (*Node, error) {
	node = node.UnsafeCopy()
	if err := unbindJobFromNodeInPlace(priorityClasses, job, node); err != nil {
		return nil, err
	}
	return node, nil
}

// unbindPodFromNodeInPlace is like UnbindJobFromNode, but doesn't make a copy of node.
func unbindJobFromNodeInPlace(priorityClasses map[string]types.PriorityClass, job interfaces.LegacySchedulerJob, node *Node) error {
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
	priority := priorityClasses[job.GetPriorityClassName()].Priority
	if isEvicted {
		priority = evictedPriority
	}
	allocatable.MarkAllocatableV1ResourceList(priority, requests)

	return nil
}

// NodeTypesMatchingPod returns a slice with all node types a pod could be scheduled on.
// It also returns the number of nodes excluded by reason for exclusion.
func (nodeDb *NodeDb) NodeTypesMatchingPod(req *schedulerobjects.PodRequirements) ([]*schedulerobjects.NodeType, map[string]int, error) {
	selectedNodeTypes := make([]*schedulerobjects.NodeType, 0)
	numExcludedNodesByReason := make(map[string]int)
	for _, nodeType := range nodeDb.nodeTypes {
		matches, reason, err := nodeType.PodRequirementsMet(req)
		if err != nil {
			return nil, nil, err
		}
		if matches {
			selectedNodeTypes = append(selectedNodeTypes, nodeType)
		} else if reason != nil {
			s := nodeDb.stringFromPodRequirementsNotMetReason(reason)
			numExcludedNodesByReason[s] += nodeDb.numNodesByNodeType[nodeType.Id]
		} else {
			numExcludedNodesByReason[schedulerobjects.PodRequirementsNotMetReasonUnknown] += nodeDb.numNodesByNodeType[nodeType.Id]
		}
	}
	return selectedNodeTypes, numExcludedNodesByReason, nil
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
		keys[i] = nodeDb.nodeDbKey(keys[i], node.NodeTypeId, node.AllocatableByPriority[p])
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

func nodeDbSchema(priorities []int32, resources []string) (*memdb.DBSchema, map[int32]string) {
	nodesTable, indexNameByPriority := nodesTableSchema(priorities, resources)
	evictionsTable := evictionsTableSchema()
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			nodesTable.Name:     nodesTable,
			evictionsTable.Name: evictionsTable,
		},
	}, indexNameByPriority
}

func nodesTableSchema(priorities []int32, resources []string) (*memdb.TableSchema, map[int32]string) {
	indexes := make(map[string]*memdb.IndexSchema, len(priorities)+1)
	indexes["id"] = &memdb.IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: &memdb.StringFieldIndex{Field: "Id"},
	}
	indexNameByPriority := make(map[int32]string, len(priorities))
	for i, priority := range priorities {
		name := nodeIndexName(i)
		indexNameByPriority[priority] = name
		indexes[name] = &memdb.IndexSchema{
			Name:    name,
			Unique:  false,
			Indexer: &NodeIndex{KeyIndex: i},
		}
	}
	return &memdb.TableSchema{
		Name:    "nodes",
		Indexes: indexes,
	}, indexNameByPriority
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
func (nodeDb *NodeDb) stringFromPodRequirementsNotMetReason(reason schedulerobjects.PodRequirementsNotMetReason) string {
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
func (nodeDb *NodeDb) nodeDbKey(out []byte, nodeTypeId uint64, allocatable schedulerobjects.ResourceList) []byte {
	return RoundedNodeIndexKeyFromResourceList(
		out,
		nodeTypeId,
		nodeDb.indexedResources,
		nodeDb.indexedResourceResolutionMillis,
		allocatable,
	)
}
