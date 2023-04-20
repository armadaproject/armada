package nodedb

import (
	"fmt"
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
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// evictedPriority is the priority class priority resources consumed by evicted jobs are accounted for at.
// This helps avoid scheduling new jobs onto nodes that make it impossible to re-schedule evicted jobs.
const evictedPriority int32 = -1

type NodeDbJob interface {
	GetId() string
	GetQueue() string
	GetJobSet() string
	GetPriorityClassName() string
	GetRequests() schedulerobjects.ResourceList
}

// NodeDb is the scheduler-internal system for storing node information.
// It's used to efficiently find nodes on which a pod can be scheduled.
type NodeDb struct {
	// In-memory database. Stores *SchedulerNode.
	// Used to efficiently iterate over nodes in sorted order.
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
	indexedResources map[string]interface{}
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
	numNodesByNodeType map[string]int
	// Total amount of resources, e.g., "cpu", "memory", "gpu", across all nodes in the db.
	totalResources schedulerobjects.ResourceList
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	nodeTypes map[string]*schedulerobjects.NodeType
	// Mutex to control access to totalResources and NodeTypes.
	mu sync.Mutex
}

func NewNodeDb(
	priorityClasses map[string]configuration.PriorityClass,
	maxExtraNodesToConsider uint,
	indexedResources,
	indexedTaints,
	indexedNodeLabels []string,
) (*NodeDb, error) {
	allowedPriorities := map[int32]bool{evictedPriority: true}
	for _, pc := range priorityClasses {
		allowedPriorities[pc.Priority] = true
	}
	prioritiesToTryAssigningAt := maps.Keys(allowedPriorities)
	slices.Sort(prioritiesToTryAssigningAt)

	db, err := memdb.NewMemDB(nodeDbSchema(
		prioritiesToTryAssigningAt,
		indexedResources,
	))
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
		indexedResources:           mapFromSlice(indexedResources),
		indexedTaints:              mapFromSlice(indexedTaints),
		indexedNodeLabels:          mapFromSlice(indexedNodeLabels),
		nodeTypes:                  make(map[string]*schedulerobjects.NodeType),
		numNodesByNodeType:         make(map[string]int),
		totalResources:             schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)},
		db:                         db,
	}, nil
}

func (nodeDb *NodeDb) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Priorities:\t%v\n", configuration.AllowedPriorities(nodeDb.priorityClasses))
	fmt.Fprintf(w, "Indexed resources:\t%v\n", maps.Keys(nodeDb.indexedResources))
	fmt.Fprintf(w, "Indexed taints:\t%v\n", maps.Keys(nodeDb.indexedTaints))
	fmt.Fprintf(w, "Indexed node labels:\t%v\n", maps.Keys(nodeDb.indexedNodeLabels))
	if len(nodeDb.nodeTypes) == 0 {
		fmt.Fprint(w, "Node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Node types:\n")
		for _, nodeType := range nodeDb.nodeTypes {
			fmt.Fprintf(w, "  %s\n", nodeType.Id)
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

	// The scheduler excludes nodes with too little of the dominant resource.
	// The dominant resource is the one for which the pod requests
	// the largest fraction of total resources.
	dominantResourceType := nodeDb.dominantResource(req)
	if dominantResourceType == "" {
		return nil, errors.Errorf("requests include no indexed resource: %v", req.ResourceRequirements.Requests)
	}

	// Create a pctx to be returned to the caller.
	pctx := &schedulercontext.PodSchedulingContext{
		Created:                  time.Now(),
		DominantResourceType:     dominantResourceType,
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
			requestForDominantResourceType := req.ResourceRequirements.Requests[v1.ResourceName(dominantResourceType)]
			reason := fmt.Sprintf(
				"less than %s %s available",
				requestForDominantResourceType.String(),
				pctx.DominantResourceType,
			)
			pctx.NumExcludedNodesByReason[reason] += numImplicitlyExcludedNodes
		}
	}()

	// If the targetNodeIdAnnocation is set, consider only that node,
	// and schedule onto that node even if it requires preempting other jobs.
	//
	// TODO: No need to even use the NodeDb here. For evicted jobs we can just get the node outside the NodeDb.
	if nodeId, ok := req.Annotations[schedulerconfig.TargetNodeIdAnnotation]; ok {
		if it, err := txn.Get("nodes", "id", nodeId); err != nil {
			return nil, errors.WithStack(err)
		} else {
			if _, err := nodeDb.selectNodeForPodWithIt(pctx, it, req.Priority, req); err != nil {
				return nil, err
			} else {
				return pctx, nil
			}
		}
	}

	// Otherwise, try to schedule this pod normally.
	// To, when possible, avoid preempting running jobs,
	// try to schedule at each available priority class priority from lowest to highest.
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
	// So we can later reset pctx.NumExcludedNodesByReason if necessary.
	numExcludedNodesByReason := maps.Clone(pctx.NumExcludedNodesByReason)

	// First try to schedule onto a node with jobs only from this queue.
	queue, err := QueueFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	if it, err := NewNodeTypesResourceIterator(
		txn,
		queue, 1,
		pctx.DominantResourceType, priority,
		pctx.MatchingNodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(pctx.DominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := nodeDb.selectNodeForPodWithIt(pctx, it, priority, req); err != nil {
			return nil, err
		} else if node != nil {
			return node, nil
		}
	}

	// Then try to schedule onto an empty node.
	if it, err := NewNodeTypesResourceIterator(
		txn,
		"", 0,
		pctx.DominantResourceType, priority,
		pctx.MatchingNodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(pctx.DominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := nodeDb.selectNodeForPodWithIt(pctx, it, priority, req); err != nil {
			return nil, err
		} else if node != nil {
			return node, nil
		}
	}

	// Finally, try to schedule onto any node.
	// Reset NumExcludedNodesByReason to avoid double-counting nodes
	// (this step may consider nodes again).
	// TODO: We could avoid needing to reset by having a smarter iterator.
	pctx.NumExcludedNodesByReason = numExcludedNodesByReason
	if it, err := NewNodeTypesResourceIterator(
		txn,
		NodeDominantQueueWildcard, 0,
		pctx.DominantResourceType, priority,
		pctx.MatchingNodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(pctx.DominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := nodeDb.selectNodeForPodWithIt(pctx, it, priority, req); err != nil {
			return nil, err
		} else if node != nil {
			return node, nil
		}
	}

	return nil, nil
}

func (nodeDb *NodeDb) selectNodeForPodWithIt(
	pctx *schedulercontext.PodSchedulingContext,
	it memdb.ResultIterator,
	priority int32,
	req *schedulerobjects.PodRequirements,
) (*schedulerobjects.Node, error) {
	var selectedNode *schedulerobjects.Node
	var selectedNodeScore int
	var numConsideredNodes uint
	for obj := it.Next(); obj != nil; obj = it.Next() {
		node := obj.(*schedulerobjects.Node)
		if node == nil {
			return nil, nil
		}
		matches, score, reason, err := node.PodRequirementsMet(priority, req)
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
			pctx.NumExcludedNodesByReason[reason.String()] += 1
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
func (nodeDb *NodeDb) BindJobToNode(job NodeDbJob, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	jobId := job.GetId()
	queue := job.GetQueue()
	requests := job.GetRequests()
	priorityClassName := job.GetPriorityClassName()
	priority := nodeDb.priorityClasses[priorityClassName].Priority
	_, isEvicted := node.EvictedJobRunIds[jobId]
	node = node.DeepCopy()

	if !isEvicted {
		if node.AllocatedByJobId == nil {
			node.AllocatedByJobId = make(map[string]schedulerobjects.ResourceList)
		}
		if allocatedToJob, ok := node.AllocatedByJobId[jobId]; ok {
			return nil, errors.Errorf("job %s already has resources allocated on node %s", jobId, node.Id)
		} else {
			allocatedToJob.Add(requests)
			node.AllocatedByJobId[jobId] = allocatedToJob
		}
		if node.AllocatedByQueue == nil {
			node.AllocatedByQueue = make(map[string]schedulerobjects.ResourceList)
		}
		allocatedToQueue := node.AllocatedByQueue[queue]
		allocatedToQueue.Add(requests)
		node.AllocatedByQueue[queue] = allocatedToQueue
	}
	delete(node.EvictedJobRunIds, jobId)

	if isEvicted {
		schedulerobjects.AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).MarkAllocatable(evictedPriority, requests)
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(priority, requests)
	if !node.AllocatableByPriorityAndResource[priority].IsStrictlyNonNegative() {
		return nil, errors.Errorf("can not bind job %s to node %s: insufficient resources available", jobId, node.Id)
	}
	return node, nil
}

// EvictPodFromNode returns a copy of node with req evicted from it. Specifically:
// - The job is marked as evicted on the node.
// - AllocatedByJobId and AllocatedByQueue are not updated.
// - Resources requested by the evicted pod are marked as allocated at priority evictedPriority.
func (nodeDb *NodeDb) EvictJobFromNode(job NodeDbJob, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	jobId := job.GetId()
	queue := job.GetQueue()
	requests := job.GetRequests()
	priorityClassName := job.GetPriorityClassName()
	priority := nodeDb.priorityClasses[priorityClassName].Priority
	node = node.DeepCopy()

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
	).MarkAllocatable(priority, requests)
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(evictedPriority, requests)
	return node, nil
}

// UnbindPodsFromNode returns a node with all reqs unbound from it.
func (nodeDb *NodeDb) UnbindPodsFromNode(jobs []NodeDbJob, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	node = node.DeepCopy()
	for _, job := range jobs {
		if err := nodeDb.unbindJobFromNodeInPlace(job, node); err != nil {
			return nil, err
		}
	}
	return node, nil
}

// UnbindPodFromNode returns a copy of node with req unbound from it.
func (nodeDb *NodeDb) UnbindPodFromNode(job NodeDbJob, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	node = node.DeepCopy()
	if err := nodeDb.unbindJobFromNodeInPlace(job, node); err != nil {
		return nil, err
	}
	return node, nil
}

// unbindPodFromNodeInPlace is like UnbindPodFromNode, but doesn't make a copy of the node.
func (nodeDb *NodeDb) unbindJobFromNodeInPlace(job NodeDbJob, node *schedulerobjects.Node) error {
	jobId := job.GetId()
	queue := job.GetQueue()
	requests := job.GetRequests()
	priorityClassName := job.GetPriorityClassName()
	priority := nodeDb.priorityClasses[priorityClassName].Priority
	_, isEvicted := node.EvictedJobRunIds[jobId]

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.Id)
	} else {
		delete(node.AllocatedByJobId, jobId)
	}
	if allocatedToQueue, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	} else {
		allocatedToQueue.Sub(requests)
		if allocatedToQueue.Equal(schedulerobjects.ResourceList{}) {
			delete(node.AllocatedByQueue, queue)
		} else {
			node.AllocatedByQueue[queue] = allocatedToQueue
		}
	}
	delete(node.EvictedJobRunIds, jobId)

	if isEvicted {
		priority = evictedPriority
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatable(priority, requests)
	return nil
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
	requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

	if !isEvicted {
		if node.AllocatedByJobId == nil {
			node.AllocatedByJobId = make(map[string]schedulerobjects.ResourceList)
		}
		if allocatedToJob, ok := node.AllocatedByJobId[jobId]; ok {
			return nil, errors.Errorf("job %s already has resources allocated on node %s", jobId, node.Id)
		} else {
			allocatedToJob.Add(requests)
			node.AllocatedByJobId[jobId] = allocatedToJob
		}
		if node.AllocatedByQueue == nil {
			node.AllocatedByQueue = make(map[string]schedulerobjects.ResourceList)
		}
		allocatedToQueue := node.AllocatedByQueue[queue]
		allocatedToQueue.Add(requests)
		node.AllocatedByQueue[queue] = allocatedToQueue
	}
	delete(node.EvictedJobRunIds, jobId)

	if isEvicted {
		schedulerobjects.AllocatableByPriorityAndResourceType(
			node.AllocatableByPriorityAndResource,
		).MarkAllocatable(evictedPriority, requests)
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(req.Priority, requests)
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
	requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

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
	).MarkAllocatable(req.Priority, requests)
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(evictedPriority, requests)
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
	requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

	if _, ok := node.AllocatedByJobId[jobId]; !ok {
		return errors.Errorf("job %s has no resources allocated on node %s", jobId, node.Id)
	} else {
		delete(node.AllocatedByJobId, jobId)
	}
	if allocatedToQueue, ok := node.AllocatedByQueue[queue]; !ok {
		return errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	} else {
		allocatedToQueue.Sub(requests)
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
	).MarkAllocatable(priority, requests)
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
			numExcludedNodesByReason[reason.String()] += nodeDb.numNodesByNodeType[nodeType.Id]
		} else {
			numExcludedNodesByReason["unknown"] += nodeDb.numNodesByNodeType[nodeType.Id]
		}
	}
	return selectedNodeTypes, numExcludedNodesByReason, nil
}

func (nodeDb *NodeDb) dominantResource(req *schedulerobjects.PodRequirements) string {
	dominantResourceType := ""
	dominantResourceFraction := 0.0
	for t, q := range req.ResourceRequirements.Requests {
		// Skip any resource types that are not indexed.
		if _, ok := nodeDb.indexedResources[string(t)]; !ok {
			continue
		}
		available, ok := nodeDb.totalResources.Resources[string(t)]
		if !ok {
			return string(t)
		}

		f := armadaresource.QuantityAsFloat64(q) / armadaresource.QuantityAsFloat64(available)
		if f >= dominantResourceFraction {
			dominantResourceType = string(t)
			dominantResourceFraction = f
		}
	}
	return dominantResourceType
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

// TODO: Let's auto-add a label to each node with the id of the node.
// For efficiency, we'd also need a special index on labels for which there are very few node per label value (maybe even unique).
func (nodeDb *NodeDb) UpsertWithTxn(txn *memdb.Txn, node *schedulerobjects.Node) error {
	// Mutating the node once inserted is forbidden.
	node = node.DeepCopy()

	// Compute the node type of the node
	// and update the node with the node accordingly.
	nodeType := schedulerobjects.NewNodeType(
		node.GetTaints(),
		node.GetLabels(),
		nodeDb.indexedTaints,
		nodeDb.indexedNodeLabels,
	)
	node.NodeTypeId = nodeType.Id
	node.NodeType = nodeType

	// If this is a new node, increase the overall resource count.
	// Note that nodeDb.totalResources isn't rolled back on txn abort.
	isNewNode := false
	if existingNode, err := nodeDb.GetNodeWithTxn(txn, node.Id); err != nil {
		return err
	} else if existingNode == nil {
		isNewNode = true
	}

	// Add the node to the db.
	if err := txn.Insert("nodes", node); err != nil {
		return errors.WithStack(err)
	}

	// Record time of the most recent upsert and all unique node types.
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

func nodeDbSchema(priorities []int32, resources []string) *memdb.DBSchema {
	indexes := make(map[string]*memdb.IndexSchema)
	indexes["id"] = &memdb.IndexSchema{
		Name:    "id",
		Unique:  true,
		Indexer: &memdb.StringFieldIndex{Field: "Id"},
	}
	for _, priority := range priorities {
		for _, resource := range resources {
			name := nodeResourcePriorityIndexName(resource, priority)
			indexes[name] = &memdb.IndexSchema{
				Name:   name,
				Unique: false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{Field: "NodeTypeId"},
						&NodeAvailableResourceIndex{
							Resource: resource,
							Priority: priority,
						},
					},
				},
			}

			name = nodeDominantQueueResourcePriorityIndexName(resource, priority)
			indexes[name] = &memdb.IndexSchema{
				Name:   name,
				Unique: false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&NodeDominantQueueIndex{},
						&memdb.StringFieldIndex{Field: "NodeTypeId"},
						&NodeAvailableResourceIndex{
							Resource: resource,
							Priority: priority,
						},
					},
				},
			}
		}
	}
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"nodes": {
				Name:    "nodes",
				Indexes: indexes,
			},
		},
	}
}

func nodeDominantQueueResourcePriorityIndexName(resource string, priority int32) string {
	return fmt.Sprintf("queue-%s", nodeResourcePriorityIndexName(resource, priority))
}

func nodeResourcePriorityIndexName(resource string, priority int32) string {
	return fmt.Sprintf("%d-%s", priority, resource)
}
