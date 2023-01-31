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
	// Time at which the most recent upsert took place.
	timeOfMostRecentUpsert time.Time
	// Allowed priority classes in sorted order.
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
		maps.Values(configuration.PrioritiesFromPriorityClasses(priorityClasses)),
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
	// slices.Sort(priorities) // To enable binary search. TODO: Do we need this?
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
	fmt.Fprintf(w, "Priorities:\t%v\n", configuration.PrioritiesFromPriorityClasses(nodeDb.priorityClasses))
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
	if node, ok := obj.(*schedulerobjects.Node); !ok {
		panic(fmt.Sprintf("expected *Node, but got %T", obj))
	} else {
		return node, nil
	}
}

func (nodeDb *NodeDb) Diff(txnA, txnB *memdb.Txn) error {

	return nil
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
			err = nodeDb.BindPodToNode(txn, req, report.Node)
			if err != nil {
				return nil, false, err
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
		err = nodeDb.BindPodToNode(txn, req, report.Node)
		if err != nil {
			return nil, err
		}
	}
	return report, nil
}

func (nodeDb *NodeDb) SelectNodeForPod(req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	return nodeDb.SelectNodeForPodWithTxn(nodeDb.db.Txn(false), req)
}

// SelectAndBindNodeToPod selects a node on which the pod can be scheduled,
// and updates the internal state of the db to indicate that this pod is bound to that node.
func (nodeDb *NodeDb) SelectNodeForPodWithTxn(txn *memdb.Txn, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	// Collect all node types that could potentially schedule the pod.
	nodeTypes, numExcludedNodeTypesByReason, err := nodeDb.NodeTypesMatchingPod(req)
	if err != nil {
		return nil, err
	}

	// Store number of nodes excluded by reason.
	numExcludedNodesByReason := make(map[string]int)

	// The dominant resource is the one for which the pod requests
	// the largest fraction of available resources.
	// For efficiency, the scheduler only considers nodes with enough of the dominant resource.
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
		NumExcludedNodesByReason:     numExcludedNodesByReason,
	}

	// Iterate over candidate nodes.
	// If the targetNodeIdAnnocation is set, only the node with that id is considered.
	// Otherwise, iterate over all nodes with enough of the dominant resource available.
	var nodeIt memdb.ResultIterator
	if req.Annotations != nil && TargetNodeIdAnnotation != "" {
		if nodeId, ok := req.Annotations[TargetNodeIdAnnotation]; ok {
			it, err := txn.Get("nodes", "id", nodeId)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			nodeIt = it
		}
	}
	if nodeIt == nil {
		it, err := NewNodeTypesResourceIterator(
			txn,
			dominantResourceType,
			req.Priority,
			nodeTypes,
			req.ResourceRequirements.Requests[v1.ResourceName(dominantResourceType)],
		)
		if err != nil {
			return nil, err
		}
		nodeIt = it
	}
	for obj := nodeIt.Next(); obj != nil; obj = nodeIt.Next() {
		node := obj.(*schedulerobjects.Node)
		if node == nil {
			break
		}
		// TODO: Use the score when selecting a node.
		matches, score, reason, err := node.PodRequirementsMet(req)
		if err != nil {
			return nil, err
		}
		if !matches {
			numExcludedNodesByReason[reason.String()] += 1
			continue
		}

		report.Node = node
		report.Score = score
		return report, nil
	}
	return report, nil
}

func (nodeDb *NodeDb) BindPodToNode(txn *memdb.Txn, req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) error {
	jobId, err := nodeDb.JobIdFromPodRequirements(req)
	if err != nil {
		return err
	}
	node = node.DeepCopy()
	requests := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
	if node.AllocatedByJobId == nil {
		node.AllocatedByJobId = make(map[string]schedulerobjects.ResourceList)
	}
	if allocatedToJob, ok := node.AllocatedByJobId[jobId]; ok {
		return errors.Errorf("job %s already has resources allocated on node %s", jobId, node.Id)
	} else {
		allocatedToJob.Add(requests)
		node.AllocatedByJobId[jobId] = allocatedToJob
	}
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocated(req.Priority, requests)
	if err := txn.Insert("nodes", node); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// UnbindPodFromNode unbinds a pod from a node, inserts into the memdb, and returns the inserted node.
// The provided node is not mutated.
func (nodeDb *NodeDb) UnbindPodFromNode(txn *memdb.Txn, req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) (*schedulerobjects.Node, error) {
	jobId, err := nodeDb.JobIdFromPodRequirements(req)
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
	schedulerobjects.AllocatableByPriorityAndResourceType(
		node.AllocatableByPriorityAndResource,
	).MarkAllocatable(req.Priority, requests)
	if err := txn.Insert("nodes", node); err != nil {
		return nil, errors.WithStack(err)
	}
	return node, nil
}

func (nodeDb *NodeDb) JobIdFromPodRequirements(req *schedulerobjects.PodRequirements) (string, error) {
	jobId, ok := req.Annotations[JobIdAnnotation]
	if !ok {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "req.Annotations",
			Value:   req.Annotations,
			Message: fmt.Sprintf("%s annotation missing", JobIdAnnotation),
		})
	}
	if jobId == "" {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "jobId",
			Value:   jobId,
			Message: "jobId is empty",
		})
	}
	return jobId, nil
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

// Upsert nodes.
func (nodeDb *NodeDb) Upsert(nodes []*schedulerobjects.Node) error {
	txn := nodeDb.db.Txn(true)
	defer txn.Abort()
	for _, node := range nodes {

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

		// Record all unique node types.
		nodeDb.mu.Lock()
		nodeDb.nodeTypes[nodeType.Id] = nodeType
		nodeDb.mu.Unlock()

		// If this is a new node, increase the overall resource count.
		it, err := nodeDb.db.Txn(false).Get("nodes", "id", node.Id)
		if err != nil {
			return errors.WithStack(err)
		}
		if it.Next() == nil {
			nodeDb.mu.Lock()
			nodeDb.totalResources.Add(node.TotalResources)
			nodeDb.mu.Unlock()
		}

		// Add the node to the db.
		err = txn.Insert("nodes", node)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	nodeDb.mu.Lock()
	nodeDb.timeOfMostRecentUpsert = time.Now()
	nodeDb.mu.Unlock()
	txn.Commit()
	return nil
}

func (nodeDb *NodeDb) TimeOfMostRecentUpsert() time.Time {
	nodeDb.mu.Lock()
	defer nodeDb.mu.Unlock()
	return nodeDb.timeOfMostRecentUpsert
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
			maps.Values(configuration.PrioritiesFromPriorityClasses(nodeDb.priorityClasses)),
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
						&NodeItemAvailableResourceIndex{
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

func nodeResourcePriorityIndexName(resource string, priority int32) string {
	return fmt.Sprintf("%d-%s", priority, resource)
}
