package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// NodeDb is the scheduler-internal system for storing node information.
// It's used to efficiently find nodes on which a pod can be scheduled.
type NodeDb struct {
	// In-memory database. Stores *SchedulerNode.
	// Used to efficiently iterate over nodes in sorted order.
	db *memdb.MemDB
	// Allowed priority classes..
	// Because the number of database indices scales linearly with the number of distinct priorities,
	// the efficiency of the NodeDb relies on the number of distinct priorities being small.
	priorityClasses map[string]configuration.PriorityClass
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
	// Total amount of resources, e.g., "cpu", "memory", "gpu", managed by the scheduler.
	// Computed approximately by periodically scanning all nodes in the db.
	totalResources schedulerobjects.ResourceList
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	nodeTypes map[string]*schedulerobjects.NodeType
	// Mutex to control access to totalResources and NodeTypes.
	mu sync.Mutex
}

func NewNodeDb(priorityClasses map[string]configuration.PriorityClass, indexedResources, indexedTaints, indexedNodeLabels []string) (*NodeDb, error) {
	db, err := memdb.NewMemDB(nodeDbSchema(
		configuration.AllowedPriorities(priorityClasses),
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
		priorityClasses:   priorityClasses,
		indexedResources:  mapFromSlice(indexedResources),
		indexedTaints:     mapFromSlice(indexedTaints),
		indexedNodeLabels: mapFromSlice(indexedNodeLabels),
		nodeTypes:         make(map[string]*schedulerobjects.NodeType),
		totalResources:    schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)},
		db:                db,
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
func (nodeDb *NodeDb) ScheduleMany(reqs []*schedulerobjects.PodRequirements) ([]*PodSchedulingReport, bool, error) {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	reports, ok, err := nodeDb.ScheduleManyWithTxn(txn, reqs)
	if ok && err == nil {
		// All pods can be scheduled; commit the transaction.
		txn.Commit()
	}
	return reports, ok, err
}

func (nodeDb *NodeDb) ScheduleManyWithTxn(txn *memdb.Txn, reqs []*schedulerobjects.PodRequirements) ([]*PodSchedulingReport, bool, error) {
	// Attempt to schedule pods one by one in a transaction.
	reports := make([]*PodSchedulingReport, 0, len(reqs))
	for _, req := range reqs {
		report, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
		if err != nil {
			return nil, false, err
		}
		reports = append(reports, report)

		// If we found a node for this pod,
		// bind it and continue to the next pod.
		//
		// Otherwise, zero out the node binding in all previous reports,
		// abort the transaction, and return.
		if report.Node != nil {
			if node, err := BindPodToNode(req, report.Node); err != nil {
				return nil, false, err
			} else {
				if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
					return nil, false, err
				}
				report.Node = node
			}
		} else {
			return reports, false, nil
		}
	}
	return reports, true, nil
}

func (nodeDb *NodeDb) SelectAndBindNodeToPod(req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	report, err := nodeDb.SelectAndBindNodeToPodWithTxn(txn, req)
	if err != nil {
		return nil, err
	}
	txn.Commit()
	return report, nil
}

func (nodeDb *NodeDb) SelectAndBindNodeToPodWithTxn(txn *memdb.Txn, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	report, err := nodeDb.SelectNodeForPodWithTxn(txn, req)
	if err != nil {
		return nil, err
	}
	if report.Node != nil {
		if node, err := BindPodToNode(req, report.Node); err != nil {
			return nil, err
		} else {
			if err := nodeDb.UpsertWithTxn(txn, node); err != nil {
				return nil, err
			}
			report.Node = node
		}
	}
	return report, nil
}

func (nodeDb *NodeDb) SelectNodeForPod(req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	return nodeDb.SelectNodeForPodWithTxn(nodeDb.db.Txn(false), req)
}

// SelectNodeForPodWithTxn selects a node on which the pod can be scheduled.
func (nodeDb *NodeDb) SelectNodeForPodWithTxn(txn *memdb.Txn, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	// Collect all node types that could potentially schedule the pod.
	nodeTypes, numExcludedNodeTypesByReason, err := nodeDb.NodeTypesMatchingPod(req)
	if err != nil {
		return nil, err
	}

	// The scheduler excludes nodes with too little of the dominant resource.
	// The dominant resource is the one for which the pod requests
	// the largest fraction of available resources.
	dominantResourceType := nodeDb.dominantResource(req)
	if dominantResourceType == "" {
		return nil, errors.Errorf("requests include no indexed resource: %v", req.ResourceRequirements.Requests)
	}

	// Create a report to be returned to the caller.
	report := &PodSchedulingReport{
		Timestamp:                    time.Now(),
		Req:                          req,
		DominantResourceType:         dominantResourceType,
		NumMatchedNodeTypes:          len(nodeTypes),
		NumExcludedNodeTypesByReason: numExcludedNodeTypesByReason,
		NumExcludedNodesByReason:     make(map[string]int),
	}

	// If the targetNodeIdAnnocation is set, consider only that node.
	if nodeId, ok := req.Annotations[TargetNodeIdAnnotation]; ok {
		if it, err := txn.Get("nodes", "id", nodeId); err != nil {
			return nil, errors.WithStack(err)
		} else {
			if _, err := selectNodeForPodWithIt(report, it, req); err != nil {
				return nil, err
			} else {
				return report, nil
			}
		}
	}

	// Otherwise, first try to schedule onto a node with jobs only from this queue.
	queue, err := QueueFromPodRequirements(req)
	if err != nil {
		return nil, err
	}
	if it, err := NewNodeTypesResourceIterator(
		txn,
		queue, 0,
		dominantResourceType, req.Priority,
		nodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(dominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := selectNodeForPodWithIt(report, it, req); err != nil {
			return nil, err
		} else if node != nil {
			return report, nil
		}
	}

	// Then try to schedule onto a node with no jobs.
	if it, err := NewNodeTypesResourceIterator(
		txn,
		"", 0,
		dominantResourceType, req.Priority,
		nodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(dominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := selectNodeForPodWithIt(report, it, req); err != nil {
			return nil, err
		} else if node != nil {
			return report, nil
		}
	}

	// Finally, try to schedule onto any node.
	if it, err := NewNodeTypesResourceIterator(
		txn,
		NodeDominantQueueWildcard, 0,
		dominantResourceType, req.Priority,
		nodeTypes,
		req.ResourceRequirements.Requests[v1.ResourceName(dominantResourceType)],
	); err != nil {
		return nil, err
	} else {
		if node, err := selectNodeForPodWithIt(report, it, req); err != nil {
			return nil, err
		} else if node != nil {
			return report, nil
		}
	}

	return report, nil
}

func selectNodeForPodWithIt(report *PodSchedulingReport, it memdb.ResultIterator, req *schedulerobjects.PodRequirements) (*schedulerobjects.Node, error) {
	for obj := it.Next(); obj != nil; obj = it.Next() {
		node := obj.(*schedulerobjects.Node)
		if node == nil {
			return nil, nil
		}
		// TODO: Use the score when selecting a node.
		matches, score, reason, err := node.PodRequirementsMet(req)
		if err != nil {
			return nil, err
		} else if matches {
			report.Node = node
			report.Score = score
			return node, nil
		}
		report.NumExcludedNodesByReason[reason.String()] += 1
	}
	return nil, nil
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

	node = node.DeepCopy()
	requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

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

	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(req.Priority, requests)
	return node, nil
}

// UnbindPodFromNode returns a copy of node with req unbound from it.
func UnbindPodFromNode(req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
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
	} else {
		delete(node.AllocatedByJobId, jobId)
	}

	if allocatedToQueue, ok := node.AllocatedByQueue[queue]; !ok {
		return nil, errors.Errorf("queue %s has no resources allocated on node %s", queue, node.Id)
	} else {
		allocatedToQueue.Sub(requests)
		if allocatedToQueue.Equal(schedulerobjects.ResourceList{}) {
			delete(node.AllocatedByQueue, queue)
		} else {
			node.AllocatedByQueue[queue] = allocatedToQueue
		}
	}

	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatable(req.Priority, requests)
	return node, nil
}

func JobIdFromPodRequirements(req *schedulerobjects.PodRequirements) (string, error) {
	return valueFromPodRequirements(req, JobIdAnnotation)
}

func QueueFromPodRequirements(req *schedulerobjects.PodRequirements) (string, error) {
	return valueFromPodRequirements(req, QueueAnnotation)
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

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could potentially be scheduled on.
func (nodeDb *NodeDb) NodeTypesMatchingPod(req *schedulerobjects.PodRequirements) ([]*schedulerobjects.NodeType, map[string]int, error) {
	return NodeTypesMatchingPod(nodeDb.nodeTypes, req)
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could potentially be scheduled on.
func NodeTypesMatchingPod(nodeTypes map[string]*schedulerobjects.NodeType, req *schedulerobjects.PodRequirements) ([]*schedulerobjects.NodeType, map[string]int, error) {
	selectedNodeTypes := make([]*schedulerobjects.NodeType, 0)
	numNodeTypesExcludedByReason := make(map[string]int)
	for _, nodeType := range nodeTypes {
		matches, reason, err := nodeType.PodRequirementsMet(req)
		if err != nil {
			return nil, nil, err
		}
		if matches {
			selectedNodeTypes = append(selectedNodeTypes, nodeType)
		} else if reason != nil {
			numNodeTypesExcludedByReason[reason.String()] += 1
		} else {
			numNodeTypesExcludedByReason["unknown reason"] += 1
		}
	}
	return selectedNodeTypes, numNodeTypesExcludedByReason, nil
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
