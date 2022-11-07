package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

// NodeDb is the scheduler-internal system for storing node information.
// It's used to efficiently find nodes on which a pod can be scheduled.
type NodeDb struct {
	// In-memory database. Stores *SchedulerNode.
	// Used to efficiently iterate over nodes in sorted order.
	Db *memdb.MemDB
	// Allowed pod priorities in sorted order.
	// Because the number of database indices scales linearly with the number of distinct priorities,
	// the efficiency of the NodeDb relies on the number of distinct priorities being small.
	priorities []int32
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
	totalResources map[string]resource.Quantity
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	NodeTypes map[string]*schedulerobjects.NodeType
	// Resources allocated by the scheduler to in-flight jobs,
	// i.e., jobs for which resource usage is not yet reported by the executor.
	inFlightResourcesByNode map[string]schedulerobjects.AllocatedByPriorityAndResourceType
	// Map from job id to the set of nodes on which that job has been assigned resources.
	// Used to clear AssignedByNode once jobs start running.
	NodesByJob map[uuid.UUID]map[string]interface{}
	// Map from node id to the set of jobs that have resourced assigned to them on that node.
	// Used to clear AssignedByNode once jobs start running.
	JobsByNode map[string]map[uuid.UUID]interface{}
	// Mutex to control access to AssignedByNode, NodesByJob, and JobsByNode.
	mu sync.Mutex
}

func NewNodeDb(priorities []int32, indexedResources, indexedTaints, indexedNodeLabels []string) (*NodeDb, error) {
	db, err := memdb.NewMemDB(nodeDbSchema(priorities, indexedResources))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	priorities = slices.Clone(priorities)
	slices.Sort(priorities) // To enable binary search.
	mapFromSlice := func(vs []string) map[string]interface{} {
		rv := make(map[string]interface{})
		for _, v := range vs {
			rv[v] = true
		}
		return rv
	}
	return &NodeDb{
		priorities:              priorities,
		indexedResources:        mapFromSlice(indexedResources),
		indexedTaints:           mapFromSlice(indexedTaints),
		indexedNodeLabels:       mapFromSlice(indexedNodeLabels),
		NodeTypes:               make(map[string]*schedulerobjects.NodeType),
		totalResources:          make(map[string]resource.Quantity),
		Db:                      db,
		NodesByJob:              make(map[uuid.UUID]map[string]interface{}),
		JobsByNode:              make(map[string]map[uuid.UUID]interface{}),
		inFlightResourcesByNode: make(map[string]schedulerobjects.AllocatedByPriorityAndResourceType),
	}, nil
}

func (nodeDb *NodeDb) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Priorities:\t%v\n", nodeDb.priorities)
	fmt.Fprintf(w, "Indexed resources:\t%v\n", maps.Keys(nodeDb.indexedResources))
	fmt.Fprintf(w, "Indexed taints:\t%v\n", maps.Keys(nodeDb.indexedTaints))
	fmt.Fprintf(w, "Indexed node labels:\t%v\n", maps.Keys(nodeDb.indexedNodeLabels))
	if len(nodeDb.NodeTypes) == 0 {
		fmt.Fprint(w, "Node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Node types:\n")
		for _, nodeType := range nodeDb.NodeTypes {
			fmt.Fprintf(w, "  %s\n", nodeType.Id)
		}
	}
	nodeDb.mu.Lock()
	fmt.Fprintf(w, "Number of in-flight jobs:\t%d\n", len(nodeDb.NodesByJob))
	nodeDb.mu.Unlock()
	w.Flush()
	return sb.String()
}

func (nodeDb *NodeDb) SelectAndBindNodeToPod(jobId uuid.UUID, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	report, err := nodeDb.SelectNodeForPod(jobId, req)
	if err != nil {
		return nil, err
	}

	if report.Node != nil {
		err = nodeDb.BindNodeToPod(jobId, req, report.Node)
		if err != nil {
			return nil, err
		}
	}

	return report, nil
}

// SelectAndBindNodeToPod selects a node on which the pod can be scheduled,
// and updates the internal state of the db to indicate that this pod is bound to that node.
func (nodeDb *NodeDb) SelectNodeForPod(jobId uuid.UUID, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
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

	// Create a report to be returned to the caller.
	report := &PodSchedulingReport{
		Timestamp:                    time.Now(),
		JobId:                        jobId,
		Req:                          req,
		DominantResourceType:         dominantResourceType,
		NumMatchedNodeTypes:          len(nodeTypes),
		NumExcludedNodeTypesByReason: numExcludedNodeTypesByReason,
		NumExcludedNodesByReason:     numExcludedNodesByReason,
	}

	// Iterate over candidate nodes.
	txn := nodeDb.Db.Txn(false)
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

	for obj := it.Next(); obj != nil; obj = it.Next() {
		node := obj.(*schedulerobjects.Node)
		if node == nil {
			break
		}
		// TODO: Use the score when selecting a node.
		nodeDb.mu.Lock()
		matches, score, reason, err := node.PodRequirementsMet(req, nodeDb.inFlightResourcesByNode[node.Id])
		nodeDb.mu.Unlock()
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

func (nodeDb *NodeDb) BindNodeToPod(jobId uuid.UUID, req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) error {
	nodeDb.mu.Lock()
	defer nodeDb.mu.Unlock()

	// Record which jobs have resources assigned to them on each node.
	if m, ok := nodeDb.JobsByNode[node.Id]; ok {
		m[jobId] = true
	} else {
		nodeDb.JobsByNode[node.Id] = map[uuid.UUID]interface{}{jobId: true}
	}

	// Record which nodes each job has resources assigned to on.
	if m, ok := nodeDb.NodesByJob[jobId]; ok {
		m[node.Id] = true
	} else {
		nodeDb.NodesByJob[jobId] = map[string]interface{}{node.Id: true}
	}

	// Mark these resources as used.
	// TODO: Avoid unnecessary copy of req.ResourceRequirements.Requests.
	rs := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	for resource, quantity := range req.ResourceRequirements.Requests {
		rs.Resources[string(resource)] = quantity
	}
	if assigned, ok := nodeDb.inFlightResourcesByNode[node.Id]; ok {
		assigned.MarkAllocated(req.Priority, rs)
	} else {
		assigned = schedulerobjects.NewAllocatedByPriorityAndResourceType(nodeDb.priorities)
		assigned.MarkAllocated(req.Priority, rs)
		nodeDb.inFlightResourcesByNode[node.Id] = assigned
	}

	return nil
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could potentially be scheduled on.
func (nodeDb *NodeDb) NodeTypesMatchingPod(req *schedulerobjects.PodRequirements) ([]*schedulerobjects.NodeType, map[string]int, error) {
	return NodeTypesMatchingPod(nodeDb.NodeTypes, req)
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
		available, ok := nodeDb.totalResources[string(t)]
		if !ok {
			return string(t)
		}

		f := common.QuantityAsFloat64(q) / common.QuantityAsFloat64(available)
		if f >= dominantResourceFraction {
			dominantResourceType = string(t)
			dominantResourceFraction = f
		}
	}
	return dominantResourceType
}

// MarkJobRunning notifies the node db that this job is now running.
// When the nodes were bound to the job, resources on those nodes were marked as assigned in the node db.
// When the job is running, those resources are accounted for by the executor,
// and should no longer be marked as assigned in the node db.
//
// TODO: This only clears AssignedByNode once there are no in-flight jobs for that node.
// We could improve it to clear AssignedByNode on a per-job basis.
func (nodeDb *NodeDb) MarkJobRunning(jobId uuid.UUID) {
	for nodeId := range nodeDb.NodesByJob[jobId] {
		delete(nodeDb.JobsByNode[nodeId], jobId)
		if len(nodeDb.JobsByNode[nodeId]) == 0 {
			delete(nodeDb.inFlightResourcesByNode, nodeId)
		}
	}
	delete(nodeDb.NodesByJob, jobId)
}

// Upsert nodes.
func (nodeDb *NodeDb) Upsert(nodes []*schedulerobjects.Node) error {
	txn := nodeDb.Db.Txn(true)
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
		nodeDb.NodeTypes[nodeType.Id] = nodeType

		// If this is a new node, increase the overall resource count.
		if _, ok := nodeDb.inFlightResourcesByNode[node.Id]; !ok {
			for t, q := range node.TotalResources.Resources {
				available := nodeDb.totalResources[t]
				available.Add(q)
				nodeDb.totalResources[t] = available
			}
		}

		err := txn.Insert("nodes", node)
		if err != nil {
			return errors.WithStack(err)
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
