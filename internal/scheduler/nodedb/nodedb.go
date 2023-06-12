package nodedb

import (
	"encoding/binary"
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
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// evictedPriority is the priority class priority resources consumed by evicted jobs are accounted for at.
// This helps avoid scheduling new jobs onto nodes that make it impossible to re-schedule evicted jobs.
const evictedPriority int32 = -1

// NodeDb is the scheduler-internal system used to efficiently find nodes on which a pod could be scheduled.
type NodeDb struct {
	// In-memory database storing *schedulerobjects.Node.
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
	priorityClasses map[string]configuration.PriorityClass
	// Priorities, in increasing order, to try to schedule pods at.
	// In particular, if a pod has priority class priority p, try to schedule that pod at priority
	// prioritiesToTryAssigningAt[0], ..., prioritiesToTryAssigningAt[i],
	// for all i such that prioritiesToTryAssigningAt[i] <= the priority of the pod.
	// We do this to, when possible, avoid preempting running jobs. Includes evictedPriority.
	prioritiesToTryAssigningAt []int32
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
	// Total number of nodes in the db.
	numNodes int
	// Number of nodes in the db by node type.
	numNodesByNodeType map[uint64]int
	// Total amount of resources, e.g., "cpu", "memory", "gpu", across all nodes in the db.
	totalResources schedulerobjects.ResourceList
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	nodeTypes map[uint64]*schedulerobjects.NodeType
	// Map from podRequirementsNotMetReason Sum64() to the string representation of that reason.
	// Used to avoid allocs.
	podRequirementsNotMetReasonStringCache map[uint64]string
	// Mutex to control access to totalResources and NodeTypes.
	mu sync.Mutex
}

func NewNodeDb(
	priorityClasses map[string]configuration.PriorityClass,
	maxExtraNodesToConsider uint,
	indexedResources []configuration.IndexResource,
	indexedTaints,
	indexedNodeLabels []string,
) (*NodeDb, error) {
	allowedPriorities := map[int32]bool{evictedPriority: true}
	for _, pc := range priorityClasses {
		allowedPriorities[pc.Priority] = true
	}
	prioritiesToTryAssigningAt := maps.Keys(allowedPriorities)
	slices.Sort(prioritiesToTryAssigningAt)

	if len(indexedResources) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "indexedResources",
			Value:   indexedResources,
			Message: "there must be at least one index resource",
		})
	}
	indexedResourceNames := util.Map(indexedResources, func(v configuration.IndexResource) string { return v.Name })
	schema, indexNameByPriority := nodeDbSchema(prioritiesToTryAssigningAt, indexedResourceNames)
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
	mapFromSlice := func(vs []string) map[string]interface{} {
		rv := make(map[string]interface{})
		for _, v := range vs {
			rv[v] = true
		}
		return rv
	}

	return &NodeDb{
		priorityClasses:            priorityClasses,
		prioritiesToTryAssigningAt: prioritiesToTryAssigningAt,
		maxExtraNodesToConsider:    maxExtraNodesToConsider,
		indexedResources:           indexedResourceNames,
		indexedResourcesSet:        mapFromSlice(indexedResourceNames),
		indexedResourceResolutionMillis: util.Map(
			indexedResources,
			func(v configuration.IndexResource) int64 { return v.Resolution.MilliValue() },
		),
		indexNameByPriority: indexNameByPriority,
		indexedTaints:       mapFromSlice(indexedTaints),
		indexedNodeLabels:   mapFromSlice(indexedNodeLabels),
		nodeTypes:           make(map[uint64]*schedulerobjects.NodeType),
		numNodesByNodeType:  make(map[uint64]int),
		totalResources:      schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)},
		db:                  db,
		// Set the initial capacity (somewhat arbitrarily) to 128 reasons.
		podRequirementsNotMetReasonStringCache: make(map[uint64]string, 128),
	}, nil
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
func (nodeDb *NodeDb) GetNode(id string) (*schedulerobjects.Node, error) {
	return nodeDb.GetNodeWithTxn(nodeDb.Txn(false), id)
}

// GetNodeWithTxn returns a node in the db with given id,
// within the provided transactions.
func (nodeDb *NodeDb) GetNodeWithTxn(txn *memdb.Txn, id string) (*schedulerobjects.Node, error) {
	it, err := txn.Get("nodes", "id", id)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	obj := it.Next()
	if obj == nil {
		return nil, nil
	}
	if node, ok := obj.(*schedulerobjects.Node); !ok {
		panic(fmt.Sprintf("expected *Node, but got %T", obj))
	} else {
		return node, nil
	}
}

// NodeJobDiff compares two snapshots of the NodeDb memdb and returns
// - a map from job ids of all preempted jobs to the node they used to be on
// - a map from job ids of all scheduled jobs to the node they were scheduled on
// that happened between the two snapshots.
func NodeJobDiff(txnA, txnB *memdb.Txn) (map[string]*schedulerobjects.Node, map[string]*schedulerobjects.Node, error) {
	preempted := make(map[string]*schedulerobjects.Node)
	scheduled := make(map[string]*schedulerobjects.Node)
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

// ScheduleMany assigns a set of pods to nodes.
// The assignment is atomic, i.e., either all pods are successfully assigned to nodes or none are.
// The returned bool indicates whether assignment succeeded or not.
// TODO: Pass through contexts to support timeouts.
func (nodeDb *NodeDb) ScheduleMany(reqs []*schedulerobjects.PodRequirements) ([]*schedulercontext.PodSchedulingContext, bool, error) {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	pctxs, ok, err := nodeDb.ScheduleManyWithTxn(txn, reqs)
	if ok && err == nil {
		// All pods can be scheduled; commit the transaction.
		txn.Commit()
	} else {
		// On failure, clear the node binding.
		for _, pctx := range pctxs {
			pctx.Node = nil
		}
	}
	return pctxs, ok, err
}

func (nodeDb *NodeDb) ScheduleManyWithTxn(txn *memdb.Txn, reqs []*schedulerobjects.PodRequirements) ([]*schedulercontext.PodSchedulingContext, bool, error) {
	// Attempt to schedule pods one by one in a transaction.
	pctxs := make([]*schedulercontext.PodSchedulingContext, 0, len(reqs))
	for _, req := range reqs {
		pctx, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
		if err != nil {
			return nil, false, err
		}
		pctxs = append(pctxs, pctx)

		// If we found a node for this pod, bind it and continue to the next pod.
		//
		// Otherwise, zero out the node binding for all pods and abort the transaction.
		if pctx.Node != nil {
			if node, err := BindPodToNode(req, pctx.Node); err != nil {
				return nil, false, err
			} else {
				if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
					return nil, false, err
				}
				pctx.Node = node
			}
		} else {
			return pctxs, false, nil
		}
	}
	return pctxs, true, nil
}

func (nodeDb *NodeDb) SelectAndBindNodeToPod(req *schedulerobjects.PodRequirements) (*schedulercontext.PodSchedulingContext, error) {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	pctx, err := nodeDb.SelectAndBindNodeToPodWithTxn(txn, req)
	if err != nil {
		return nil, err
	}
	txn.Commit()
	return pctx, nil
}

func (nodeDb *NodeDb) SelectAndBindNodeToPodWithTxn(txn *memdb.Txn, req *schedulerobjects.PodRequirements) (*schedulercontext.PodSchedulingContext, error) {
	pctx, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
	if err != nil {
		return nil, err
	}
	if pctx.Node != nil {
		if node, err := BindPodToNode(req, pctx.Node); err != nil {
			return nil, err
		} else {
			if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
				return nil, err
			}
			pctx.Node = node
		}
	}
	return pctx, nil
}

func (nodeDb *NodeDb) SelectNodeForPod(req *schedulerobjects.PodRequirements) (*schedulercontext.PodSchedulingContext, error) {
	return nodeDb.SelectNodeForPodWithTxn(nodeDb.db.Txn(false), req)
}

// SelectNodeForPodWithTxn selects a node on which the pod can be scheduled.
func (nodeDb *NodeDb) SelectNodeForPodWithTxn(txn *memdb.Txn, req *schedulerobjects.PodRequirements) (*schedulercontext.PodSchedulingContext, error) {
	// Collect all node types that could potentially schedule the pod.
	matchingNodeTypes, numExcludedNodesByReason, err := nodeDb.NodeTypesMatchingPod(req)
	if err != nil {
		return nil, err
	}

	// Create a pctx to be returned to the caller.
	pctx := &schedulercontext.PodSchedulingContext{
		Created:                  time.Now(),
		MatchingNodeTypes:        matchingNodeTypes,
		NumNodes:                 nodeDb.numNodes,
		NumExcludedNodesByReason: maps.Clone(numExcludedNodesByReason),
	}

	// For pods that failed to schedule, add an exclusion reason for implicitly excluded nodes.
	defer func() {
		if pctx.Node != nil {
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

	// If the targetNodeIdAnnocation is set, consider only that node,
	// and schedule onto that node even if it requires preempting other jobs.
	if nodeId, ok := req.NodeSelector[schedulerconfig.NodeIdLabel]; ok {
		if it, err := txn.Get("nodes", "id", nodeId); err != nil {
			return nil, errors.WithStack(err)
		} else {
			if _, err := nodeDb.selectNodeForPodWithIt(pctx, it, req.Priority, req, true); err != nil {
				return nil, err
			} else {
				return pctx, nil
			}
		}
	}

	// Try to schedule this pod normally.
	// To avoid preempting running jobs, try scheduling at each available priority from lowest to highest.
	for _, priority := range nodeDb.prioritiesToTryAssigningAt {
		if priority > req.Priority {
			break
		}

		// Reset NumExcludedNodesByReason to avoid double-counting nodes
		// (since we may consider all nodes at each priority).
		pctx.NumExcludedNodesByReason = maps.Clone(numExcludedNodesByReason)

		// To to find a node at this priority.
		node, err := nodeDb.selectNodeForPodAtPriority(txn, pctx, priority, req)
		if err != nil {
			return nil, err
		}
		if node != nil {
			if pctx.Node == nil {
				return nil, errors.New("pctx.Node not set")
			}
			if node.Id != pctx.Node.Id {
				return nil, errors.New("pctx.Node.Id does not match that of the returned node")
			}
			return pctx, nil
		} else if pctx.Node != nil {
			return nil, errors.New("pctx.Node is set, but no node was returned")
		}
	}
	return pctx, nil
}

func (nodeDb *NodeDb) selectNodeForPodAtPriority(
	txn *memdb.Txn,
	pctx *schedulercontext.PodSchedulingContext,
	priority int32,
	req *schedulerobjects.PodRequirements,
) (*schedulerobjects.Node, error) {
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
) (*schedulerobjects.Node, error) {
	var selectedNode *schedulerobjects.Node
	var selectedNodeScore int
	var numConsideredNodes uint
	for obj := it.Next(); obj != nil; obj = it.Next() {
		node := obj.(*schedulerobjects.Node)
		if node == nil {
			return nil, nil
		}
		var matches bool
		var score int
		var reason schedulerobjects.PodRequirementsNotMetReason
		var err error
		if onlyCheckDynamicRequirements {
			matches, score, reason, err = node.DynamicPodRequirementsMet(priority, req)
		} else {
			matches, score, reason, err = node.PodRequirementsMet(priority, req)
		}
		if err != nil {
			return nil, err
		} else if matches {
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
		if selectedNode != nil {
			numConsideredNodes++
			if numConsideredNodes == nodeDb.maxExtraNodesToConsider+1 {
				break
			}
		}
	}
	pctx.Node = selectedNode
	pctx.Score = selectedNodeScore
	return selectedNode, nil
}

// BindPodToNode returns a copy of node with req bound to it.
func BindPodToNode(req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	jobId, err := JobIdFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	queue, err := QueueFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	_, isEvicted := node.EvictedJobRunIds[jobId]

	node = node.DeepCopy()

	if !isEvicted {
		if node.AllocatedByJobId == nil {
			node.AllocatedByJobId = make(map[string]schedulerobjects.ResourceList)
		}
		if allocatedToJob, ok := node.AllocatedByJobId[jobId]; ok {
			return nil, errors.Errorf("job %s already has resources allocated on node %s", jobId, node.Id)
		} else {
			allocatedToJob.AddV1ResourceList(req.ResourceRequirements.Requests)
			node.AllocatedByJobId[jobId] = allocatedToJob
		}
		if node.AllocatedByQueue == nil {
			node.AllocatedByQueue = make(map[string]schedulerobjects.ResourceList)
		}
		allocatedToQueue := node.AllocatedByQueue[queue]
		allocatedToQueue.AddV1ResourceList(req.ResourceRequirements.Requests)
		node.AllocatedByQueue[queue] = allocatedToQueue
	}
	delete(node.EvictedJobRunIds, jobId)

	if isEvicted {
		schedulerobjects.AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).MarkAllocatableV1ResourceList(evictedPriority, req.ResourceRequirements.Requests)
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatedV1ResourceList(req.Priority, req.ResourceRequirements.Requests)
	return node, nil
}

// EvictPodFromNode returns a copy of node with req evicted from it. Specifically:
// - The job is marked as evicted on the node.
// - AllocatedByJobId and AllocatedByQueue are not updated.
// - Resources requested by the evicted pod are marked as allocated at priority evictedPriority.
func EvictPodFromNode(req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	jobId, err := JobIdFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	queue, err := QueueFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	node = node.DeepCopy()

	// Ensure we track allocated resources at evictedPriority.
	if _, ok := node.AllocatableByPriorityAndResource[evictedPriority]; !ok {
		pMin := int32(math.MaxInt32)
		ok := false
		for p := range node.AllocatableByPriorityAndResource {
			if p < pMin {
				pMin = p
				ok = true
			}
		}
		if ok {
			node.AllocatableByPriorityAndResource[evictedPriority] = node.AllocatableByPriorityAndResource[pMin].DeepCopy()
		}
	}

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return nil, errors.Errorf("job %s has no resources allocated on node %s", jobId, node.Id)
	}
	if _, ok := node.AllocatedByQueue[queue]; !ok {
		return nil, errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	}
	if node.EvictedJobRunIds == nil {
		node.EvictedJobRunIds = make(map[string]bool)
	}
	if _, ok := node.EvictedJobRunIds[jobId]; ok {
		// TODO: We're using run ids instead of job ids for now.
		return nil, errors.Errorf("job %s is already evicted from node %s", jobId, node.Id)
	} else {
		node.EvictedJobRunIds[jobId] = true
	}

	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatableV1ResourceList(req.Priority, req.ResourceRequirements.Requests)
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatedV1ResourceList(evictedPriority, req.ResourceRequirements.Requests)
	return node, nil
}

// UnbindPodsFromNode returns a node with all reqs unbound from it.
func UnbindPodsFromNode(reqs []*schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	node = node.DeepCopy()
	for _, req := range reqs {
		if err := unbindPodFromNodeInPlace(req, node); err != nil {
			return nil, err
		}
	}
	return node, nil
}

// UnbindPodFromNode returns a copy of node with req unbound from it.
func UnbindPodFromNode(req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	node = node.DeepCopy()
	if err := unbindPodFromNodeInPlace(req, node); err != nil {
		return nil, err
	}
	return node, nil
}

// unbindPodFromNodeInPlace is like UnbindPodFromNode, but doesn't make a copy of the node.
func unbindPodFromNodeInPlace(req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) error {
	jobId, err := JobIdFromPodRequirements(req)
	if err != nil {
		return err
	}
	queue, err := QueueFromPodRequirements(req)
	if err != nil {
		return err
	}
	_, isEvicted := node.EvictedJobRunIds[jobId]

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.Id)
	} else {
		delete(node.AllocatedByJobId, jobId)
	}
	if allocatedToQueue, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	} else {
		allocatedToQueue.SubV1ResourceList(req.ResourceRequirements.Requests)
		if allocatedToQueue.Equal(schedulerobjects.ResourceList{}) {
			delete(node.AllocatedByQueue, queue)
		} else {
			node.AllocatedByQueue[queue] = allocatedToQueue
		}
	}
	delete(node.EvictedJobRunIds, jobId)

	priority := req.Priority
	if isEvicted {
		priority = evictedPriority
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatableV1ResourceList(priority, req.ResourceRequirements.Requests)
	return nil
}

func JobIdFromPodRequirements(req *schedulerobjects.PodRequirements) (string, error) {
	return valueFromPodRequirements(req, schedulerconfig.JobIdAnnotation)
}

func QueueFromPodRequirements(req *schedulerobjects.PodRequirements) (string, error) {
	return valueFromPodRequirements(req, schedulerconfig.QueueAnnotation)
}

func valueFromPodRequirements(req *schedulerobjects.PodRequirements, key string) (string, error) {
	v, ok := req.Annotations[key]
	if !ok {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "req.Annotations",
			Value:   req.Annotations,
			Message: fmt.Sprintf("%s annotation missing", key),
		})
	}
	if v == "" {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    key,
			Value:   v,
			Message: fmt.Sprintf("value of %s is empty", key),
		})
	}
	return v, nil
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

func (nodeDb *NodeDb) UpsertMany(nodes []*schedulerobjects.Node) error {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	if err := nodeDb.UpsertManyWithTxn(txn, nodes); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) UpsertManyWithTxn(txn *memdb.Txn, nodes []*schedulerobjects.Node) error {
	for _, node := range nodes {
		if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
			return err
		}
	}
	return nil
}

func (nodeDb *NodeDb) Upsert(node *schedulerobjects.Node) error {
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) UpsertWithTxn(txn *memdb.Txn, node *schedulerobjects.Node) error {
	// Mutating the node once inserted is forbidden.
	// TODO: We shouldn't need a copy here.
	node = node.DeepCopy()

	// Add an evictedPriority record to the node.
	// TODO: We should make NodeDb responsible for creating new nodes and add this record at creation instead of upsert.
	if len(node.EvictedJobRunIds) != 0 {
		q := schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).Get(evictedPriority, "cpu")
		if q.Cmp(node.TotalResources.Get("cpu")) == 0 {
			return errors.Errorf("inconsistent node accounting: node %s has evicted jobs but no evicted resources", node.Id)
		}
	}

	// Ensure we track allocated resources at evictedPriority.
	if _, ok := node.AllocatableByPriorityAndResource[evictedPriority]; !ok {
		pMin := int32(math.MaxInt32)
		ok := false
		for p := range node.AllocatableByPriorityAndResource {
			if p < pMin {
				pMin = p
				ok = true
			}
		}
		if ok {
			node.AllocatableByPriorityAndResource[evictedPriority] = node.AllocatableByPriorityAndResource[pMin].DeepCopy()
		}
	}

	// Make sure nodes have a label containing the nodeId.
	if node.Labels == nil {
		node.Labels = map[string]string{schedulerconfig.NodeIdLabel: node.Id}
	} else {
		node.Labels[schedulerconfig.NodeIdLabel] = node.Id
	}

	// Add a special taint to unschedulable nodes before inserting.
	// Adding a corresponding toleration to evicted pods ensures they can be re-scheduled.
	// To prevent scheduling new pods onto cordoned nodes, only evicted pods should have this toleration.
	if node.Unschedulable {
		node.Taints = append(node.Taints, UnschedulableTaint())
	}

	// Compute the node type of the node.
	nodeType := schedulerobjects.NewNodeType(
		node.GetTaints(),
		node.GetLabels(),
		nodeDb.indexedTaints,
		nodeDb.indexedNodeLabels,
	)
	node.NodeTypeId = nodeType.Id
	node.NodeType = nodeType

	// Compute the keys necessary to efficiently iterate over nodes.
	node.NodeDbKeys = make([][]byte, len(nodeDb.prioritiesToTryAssigningAt))
	for i, p := range nodeDb.prioritiesToTryAssigningAt {
		node.NodeDbKeys[i] = nodeDb.nodeDbKeyFromNode(node.NodeDbKeys[i], node, p)
	}

	// Add the node to the db.
	isNewNode := false
	if existingNode, err := nodeDb.GetNodeWithTxn(txn, node.Id); err != nil {
		return err
	} else if existingNode == nil {
		isNewNode = true
	}
	if err := txn.Insert("nodes", node); err != nil {
		return errors.WithStack(err)
	}

	// If this is a new node, update overall statistics.
	// Note that these are not rolled back on txn abort.
	nodeDb.mu.Lock()
	if isNewNode {
		nodeDb.numNodes++
		nodeDb.numNodesByNodeType[nodeType.Id]++
		nodeDb.totalResources.Add(node.TotalResources)
	}
	nodeDb.nodeTypes[nodeType.Id] = nodeType
	nodeDb.mu.Unlock()

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
	for node := it.NextNode(); node != nil; node = it.NextNode() {
		node = node.DeepCopy()
		node.AllocatableByPriorityAndResource = schedulerobjects.NewAllocatableByPriorityAndResourceType(
			configuration.AllowedPriorities(nodeDb.priorityClasses),
			nodeDb.totalResources,
		)
		err := txn.Insert("nodes", node)
		if err != nil {
			return err
		}
	}
	txn.Commit()
	return nil
}

func nodeDbSchema(priorities []int32, resources []string) (*memdb.DBSchema, map[int32]string) {
	indexes := make(map[string]*memdb.IndexSchema)
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
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"nodes": {
				Name:    "nodes",
				Indexes: indexes,
			},
		},
	}, indexNameByPriority
}

func nodeIndexName(keyIndex int) string {
	return fmt.Sprintf("index-%d", keyIndex)
}

// stringFromPodRequirementsNotMetReason returns the string representation of reason,
// using a cache to avoid allocating new strings when possible.
func (nodeDb *NodeDb) stringFromPodRequirementsNotMetReason(reason schedulerobjects.PodRequirementsNotMetReason) string {
	h := reason.Sum64()
	if s, ok := nodeDb.podRequirementsNotMetReasonStringCache[h]; ok {
		return s
	} else {
		s := reason.String()
		nodeDb.podRequirementsNotMetReasonStringCache[h] = s
		return s
	}
}

func (nodeDb *NodeDb) nodeDbKeyFromNode(out []byte, node *schedulerobjects.Node, priority int32) []byte {
	size := 8
	out = append(out, make([]byte, size)...)
	binary.BigEndian.PutUint64(out[len(out)-size:], node.NodeTypeId)
	for i, resource := range nodeDb.indexedResources {
		resolution := nodeDb.indexedResourceResolutionMillis[i]
		q := node.AvailableQuantityByPriorityAndResource(priority, resource)
		q.SetMilli((q.MilliValue() / resolution) * resolution)
		out = schedulerobjects.EncodeQuantityBuffer(out, q)
	}
	return out
}

func appendNodeDbKey(out []byte, nodeTypeId uint64, resources []resource.Quantity, resourceResolutionMillis []int64) []byte {
	if len(resources) != len(resourceResolutionMillis) {
		panic("resources and resourceResolutionMillis do not have equal length")
	}
	size := 8
	out = append(out, make([]byte, size)...)
	binary.BigEndian.PutUint64(out[len(out)-size:], nodeTypeId)
	for i, q := range resources {
		resolution := resourceResolutionMillis[i]
		q.SetMilli((q.MilliValue() / resolution) * resolution)
		out = schedulerobjects.EncodeQuantityBuffer(out, q)
	}
	return out
}
