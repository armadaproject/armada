package scheduler

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
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
	// Total amount of resources, e.g., "cpu", "memory", "gpu", managed by the scheduler.
	// Computed approximately by periodically scanning all nodes in the db.
	// TODO: Do we need this? If so, compute it correctly.
	totalResources map[string]*resource.Quantity
	// Set of node types for which there exists at least 1 node in the db.
	NodeTypes map[string]*NodeType
	// Resources allocated by the scheduler to in-flight jobs,
	// i.e., jobs for which resource usage is not yet reported by the executor.
	AssignedByNode map[string]AssignedByPriorityAndResourceType
	// Map from job id to the set of nodes on which that job has been assigned resources.
	// Used to clear AssignedByNode once jobs start running.
	NodesByJob map[uuid.UUID]map[string]interface{}
	// Map from node id to the set of jobs that have resourced assigned to them on that node.
	// Used to clear AssignedByNode once jobs start running.
	JobsByNode map[string]map[uuid.UUID]interface{}
}

// PodSchedulingReport is returned by SelectAndBindNodeToPod and
// contains detailed information on the scheduling decision made for this pod.
type PodSchedulingReport struct {
	// Id of the job this pod corresponds to.
	JobId uuid.UUID
	// Pod scheduling requirements.
	Req *schedulerobjects.PodRequirements
	// Node the pod was assigned to.
	// If nil, the pod could not be assigned to any node.
	Node *SchedulerNode
	// Score indicates how well the pod fits on the selected node.
	Score int
	// Number of node types that
	NumMatchedNodeTypes int
	// Number of node types excluded by reason.
	NumExcludedNodeTypesByReason map[string]int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
}

// SelectAndBindNodeToPod selects a node on which the pod can be scheduled,
// and updates the internal state of the db to indicate that this pod is bound to that node.
// TODO: Maybe PodToNode.
func (nodeDb *NodeDb) SelectAndBindNodeToPod(jobId uuid.UUID, req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {

	// Collect all node types that could potentially schedule the pod.
	nodeTypes, numExcludedNodeTypesByReason, err := nodeDb.NodeTypesMatchingPod(req)
	if err != nil {
		return nil, err
	}

	// Store number of nodes excluded by reason.
	numExcludedNodesByReason := make(map[string]int)

	// Create a report to be returned to the caller.
	report := &PodSchedulingReport{
		JobId:                        jobId,
		Req:                          req,
		NumMatchedNodeTypes:          len(nodeTypes),
		NumExcludedNodeTypesByReason: numExcludedNodeTypesByReason,
		NumExcludedNodesByReason:     numExcludedNodesByReason,
	}

	// The dominant resource is the one for which the pod requests
	// the largest fraction of available resources.
	// For efficiency, the scheduler only considers nodes with enough of the dominant resource.
	dominantResourceType := nodeDb.dominantResource(req)

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
		node := obj.(*SchedulerNode)
		if node == nil {
			break
		}
		// TODO: Use the score when selecting a node.
		matches, score, reason, err := node.PodRequirementsMet(req, nodeDb.AssignedByNode[node.Id])
		if err != nil {
			return nil, err
		}
		if !matches {
			numExcludedNodesByReason[reason.String()] += 1
			continue
		}

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
		rs := make(map[string]resource.Quantity)
		for resource, quantity := range req.ResourceRequirements.Requests {
			rs[string(resource)] = quantity
		}
		if assigned, ok := nodeDb.AssignedByNode[node.Id]; ok {
			assigned.MarkUsed(req.Priority, rs)
		} else {
			assigned = NewAssignedByPriorityAndResourceType(nodeDb.priorities)
			assigned.MarkUsed(req.Priority, rs)
			nodeDb.AssignedByNode[node.Id] = assigned
		}

		report.Node = node
		report.Score = score
		return report, nil
	}
	return report, nil
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could be scheduled on, i.e., all node types with
// matching node selectors and no untolerated taints.
//
// TODO: Update docstring.
func (nodeDb *NodeDb) NodeTypesMatchingPod(req *schedulerobjects.PodRequirements) ([]*NodeType, map[string]int, error) {
	return NodeTypesMatchingPod(nodeDb.NodeTypes, req)
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could be scheduled on, i.e., all node types with
// matching node selectors and no untolerated taints.
//
// TODO: Update docstring.
func NodeTypesMatchingPod(nodeTypes map[string]*NodeType, req *schedulerobjects.PodRequirements) ([]*NodeType, map[string]int, error) {
	selectedNodeTypes := make([]*NodeType, 0)
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
		available, ok := nodeDb.totalResources[string(t)]
		if !ok {
			return string(t)
		}
		f := q.AsApproximateFloat64() / available.AsApproximateFloat64()
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
			delete(nodeDb.AssignedByNode, nodeId)
		}
	}
	delete(nodeDb.NodesByJob, jobId)
}

// SchedulerNode is a scheduler-specific representation of a node.
type SchedulerNode struct {
	// Unique name associated with the node.
	// Only used internally by the scheduler.
	Id string
	// Time at which this node was last updated.
	// Used to garbage collect nodes that have been removed.
	LastSeen time.Time
	// The node type captures scheduling requirements of the node;
	// it's computed from the taints and labels associated with the node.
	NodeType *NodeType
	// We store the NodeType.id here to simplify indexing.
	NodeTypeId string
	// Node info object received from the executor.
	// TODO: We don't need to store this. Just get what we need out if.
	NodeInfo *api.NodeInfo
	// Resources available for jobs of a given priority.
	// E.g., AvailableResources[5]["cpu"] is the amount of CPU available to jobs with priority 5,
	// where available resources = unused resources + resources assigned to lower-priority jobs.
	AvailableResources AvailableByPriorityAndResourceType
}

func (node *SchedulerNode) GetLabels() map[string]string {
	if node.NodeInfo == nil {
		return nil
	}
	return node.NodeInfo.Labels
}

func (node *SchedulerNode) GetTaints() []v1.Taint {
	if node.NodeInfo == nil {
		return nil
	}
	return node.NodeInfo.Taints
}

type QuantityByResourceType map[string]resource.Quantity

type QuantityByPriorityAndResourceType map[int32]map[string]resource.Quantity

// AvailableByPriorityAndResourceType accounts for resources available to pods of a given priority.
// E.g., AvailableByPriorityAndResourceType[5]["cpu"] is the amount of CPU available to pods with priority 5,
// where available resources = unused resources + resources assigned to lower-priority pods.
type AvailableByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAvailableByPriorityAndResourceType(priorities []int32) AvailableByPriorityAndResourceType {
	rv := make(AvailableByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = make(map[string]resource.Quantity)
	}
	return rv
}

// MarkUsed reduces the resources available to pods of priority p or lower.
func (m AvailableByPriorityAndResourceType) MarkUsed(p int32, rs map[string]resource.Quantity) {
	for priority, availableResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs {
				q := availableResourcesAtPriority[usedResourceType]
				if q.Cmp(usedResourceQuantity) == -1 {
					q.Set(0)
				} else {
					q.Sub(usedResourceQuantity)
				}
				availableResourcesAtPriority[usedResourceType] = q
			}
		}
	}
}

// MarkAvailable increases the resources available to pods of priority p or higher.
func (m AvailableByPriorityAndResourceType) MarkAvailable(p int32, rs map[string]resource.Quantity) {
	for priority, availableResourcesAtPriority := range m {
		if priority >= p {
			for usedResourceType, usedResourceQuantity := range rs {
				q := availableResourcesAtPriority[usedResourceType]
				q.Add(usedResourceQuantity)
				availableResourcesAtPriority[usedResourceType] = q
			}
		}
	}
}

// AssignedByPriorityAndResourceType accounts for resources assigned to pods of a given priority or higher.
// E.g., AssignedByPriorityAndResourceType[5]["cpu"] is the amount of CPU assigned to pods with priority 5 or higher.
type AssignedByPriorityAndResourceType QuantityByPriorityAndResourceType

func NewAssignedByPriorityAndResourceType(priorities []int32) AssignedByPriorityAndResourceType {
	rv := make(AssignedByPriorityAndResourceType)
	for _, priority := range priorities {
		rv[priority] = make(map[string]resource.Quantity)
	}
	return rv
}

// MarkUsed increases the resources assigned to pods of priority p or higher.
func (m AssignedByPriorityAndResourceType) MarkUsed(p int32, rs map[string]resource.Quantity) {
	for priority, assignedResourcesAtPriority := range m {
		if priority >= p {
			for usedResourceType, usedResourceQuantity := range rs {
				q := assignedResourcesAtPriority[usedResourceType]
				q.Add(usedResourceQuantity)
				assignedResourcesAtPriority[usedResourceType] = q
			}
		}
	}
}

// MarkAvailable reduces the resources assigned to pods of priority p or lower.
func (m AssignedByPriorityAndResourceType) MarkAvailable(p int32, rs map[string]resource.Quantity) {
	for priority, assignedResourcesAtPriority := range m {
		if priority <= p {
			for usedResourceType, usedResourceQuantity := range rs {
				q := assignedResourcesAtPriority[usedResourceType]
				if q.Cmp(usedResourceQuantity) == -1 {
					q.Set(0)
				} else {
					q.Sub(usedResourceQuantity)
				}
				assignedResourcesAtPriority[usedResourceType] = q
			}
		}
	}
}

func (availableByPriorityAndResourceType AvailableByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if availableByPriorityAndResourceType == nil {
		return resource.MustParse("0")
	}
	quantityByResourceType, ok := availableByPriorityAndResourceType[priority]
	if !ok {
		return resource.MustParse("0")
	}
	q, ok := quantityByResourceType[resourceType]
	if !ok {
		return resource.MustParse("0")
	}
	return q
}

func (assignedByPriorityAndResourceType AssignedByPriorityAndResourceType) Get(priority int32, resourceType string) resource.Quantity {
	if assignedByPriorityAndResourceType == nil {
		return resource.MustParse("0")
	}
	quantityByResourceType, ok := assignedByPriorityAndResourceType[priority]
	if !ok {
		return resource.MustParse("0")
	}
	q, ok := quantityByResourceType[resourceType]
	if !ok {
		return resource.MustParse("0")
	}
	return q
}

func (nodeItem *SchedulerNode) availableQuantityByPriorityAndResource(priority int32, resourceType string) resource.Quantity {
	return nodeItem.AvailableResources.Get(priority, resourceType)
}

func NewNodeDb(priorities []int32, resourceTypes []string) (*NodeDb, error) {
	db, err := memdb.NewMemDB(nodeDbSchema(priorities, resourceTypes))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	priorities = []int32(priorities)
	slices.Sort(priorities)
	totalResources := make(map[string]*resource.Quantity)
	for _, resourceType := range resourceTypes {
		q := resource.MustParse("0")
		totalResources[resourceType] = &q
	}
	return &NodeDb{
		priorities:     priorities,
		NodeTypes:      make(map[string]*NodeType),
		totalResources: totalResources,
		Db:             db,
		NodesByJob:     make(map[uuid.UUID]map[string]interface{}),
		JobsByNode:     make(map[string]map[uuid.UUID]interface{}),
		AssignedByNode: make(map[string]AssignedByPriorityAndResourceType),
	}, nil
}

// Upsert will update the node db with the given nodes.
func (nodeDb *NodeDb) Upsert(nodes []*SchedulerNode) error {
	maxPriority := nodeDb.priorities[len(nodeDb.priorities)-1]
	txn := nodeDb.Db.Txn(true)
	defer txn.Abort()
	for _, node := range nodes {

		// If this is a new node, increase the overall resource count.
		if _, ok := nodeDb.AssignedByNode[node.Id]; !ok {
			for t, q := range node.AvailableResources[maxPriority] {
				available := nodeDb.totalResources[t]
				if available == nil {
					q := q.DeepCopy()
					nodeDb.totalResources[t] = &q
				} else {
					available.Add(q)
					nodeDb.totalResources[t] = available
				}
			}
		}

		err := txn.Insert("nodes", node)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	txn.Commit()

	// Record all known node types.
	for _, node := range nodes {
		nodeDb.NodeTypes[node.NodeType.id] = node.NodeType
	}

	return nil
}

func (nodeDb *NodeDb) SchedulerNodeFromNodeInfo(nodeInfo *api.NodeInfo, executor string) *SchedulerNode {
	return &SchedulerNode{
		Id:                 fmt.Sprintf("%s-%s", executor, nodeInfo.Name),
		NodeType:           NewNodeTypeFromNodeInfo(nodeInfo, nil, nil),
		NodeInfo:           nodeInfo,
		AvailableResources: availableResourcesFromNodeInfo(nodeInfo, nodeDb.priorities),
	}
}

func availableResourcesFromNodeInfo(nodeInfo *api.NodeInfo, allowedPriorities []int32) AvailableByPriorityAndResourceType {
	rv := make(AvailableByPriorityAndResourceType)
	for _, priority := range allowedPriorities {
		rv[priority] = maps.Clone(nodeInfo.TotalResources)
	}
	for allocatedPriority, allocatedResources := range nodeInfo.AllocatedResources {
		for _, priority := range allowedPriorities {
			if priority <= allocatedPriority {
				for resource, quantity := range allocatedResources.Resources {
					q := rv[priority][resource]
					q.Sub(quantity)
					rv[priority][resource] = q
				}
			}
		}
	}
	return rv
}

func nodeFromNodeInfo(nodeInfo *api.NodeInfo) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: nodeInfo.GetLabels(),
		},
		Spec: v1.NodeSpec{
			Taints: nodeInfo.GetTaints(),
		},
	}
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
			name := fmt.Sprintf("%d-%s", priority, resource)
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
