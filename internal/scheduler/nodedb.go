package scheduler

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// SelectAndBindNodeToPod selects a node on which the pod can be scheduled,
// and updates the internal state of the db to indicate that this pod is bound to that node.
// TODO: Maybe PodToNode.
func (nodeDb *NodeDb) SelectAndBindNodeToPod(jobId uuid.UUID, req *PodSchedulingRequirements) (*SchedulerNode, error) {

	// Collect all node types that could schedule the pod.
	nodeTypes := nodeDb.NodeTypesMatchingPod(req)
	if len(nodeTypes) == 0 {
		return nil, errors.New("pod doesn't match any node type")
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
		req.ResourceRequirements[dominantResourceType],
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
		_, err := node.canSchedulePod(req, nodeDb.AssignedByNode[node.Id])
		if err != nil {
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
		if assigned, ok := nodeDb.AssignedByNode[node.Id]; ok {
			assigned.MarkUsed(req.Priority, req.ResourceRequirements)
		} else {
			assigned = NewAssignedByPriorityAndResourceType(nodeDb.priorities)
			assigned.MarkUsed(req.Priority, req.ResourceRequirements)
			nodeDb.AssignedByNode[node.Id] = assigned
		}

		return node, nil
	}

	// TODO: Return a more specific reason.
	return nil, errors.New("pod currently not schedulable on any node")
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could be scheduled on, i.e., all node types with
// matching node selectors and no untolerated taints.
func (nodeDb *NodeDb) NodeTypesMatchingPod(req *PodSchedulingRequirements) []*NodeType {
	return NodeTypesMatchingPod(nodeDb.NodeTypes, req)
}

// NodeTypesMatchingPod returns a slice composed of all node types
// a given pod could be scheduled on, i.e., all node types with
// matching node selectors and no untolerated taints.
func NodeTypesMatchingPod(nodeTypes map[string]*NodeType, req *PodSchedulingRequirements) []*NodeType {
	rv := make([]*NodeType, 0)
	for _, nodeType := range nodeTypes {
		if err := nodeType.canSchedulePod(req); err == nil {
			rv = append(rv, nodeType)
		}
	}
	return rv
}

func (nodeDb *NodeDb) dominantResource(req *PodSchedulingRequirements) string {
	dominantResourceType := ""
	dominantResourceFraction := 0.0
	for t, q := range req.ResourceRequirements {
		available, ok := nodeDb.totalResources[t]
		if !ok {
			return t
		}
		f := q.AsApproximateFloat64() / available.AsApproximateFloat64()
		if f >= dominantResourceFraction {
			dominantResourceType = t
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
	// The node type captures scheduling requirements of the node;
	// it's computed from the taints and labels associated with the node.
	NodeType *NodeType
	// We store the NodeType.id here to simplify indexing.
	// TODO: Remove NodeTypeId; use NodeType.Id.
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
