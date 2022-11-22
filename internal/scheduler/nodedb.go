package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/armadaerrors"
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
	totalResources schedulerobjects.ResourceList
	// Set of node types. Populated automatically as nodes are inserted.
	// Node types are not cleaned up if all nodes of that type are removed from the NodeDb.
	NodeTypes map[string]*schedulerobjects.NodeType
	// Mutex to control access to totalResources and NodeTypes.
	mu sync.Mutex
}

func NewNodeDb(priorities []int32, indexedResources, indexedTaints, indexedNodeLabels []string) (*NodeDb, error) {
	db, err := memdb.NewMemDB(nodeDbSchema(priorities, indexedResources))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(priorities) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "priorities",
			Value:   priorities,
			Message: "there must be at least one supported priority",
		})
	}
	priorities = slices.Clone(priorities)
	slices.Sort(priorities) // To enable binary search.
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
		priorities:        priorities,
		indexedResources:  mapFromSlice(indexedResources),
		indexedTaints:     mapFromSlice(indexedTaints),
		indexedNodeLabels: mapFromSlice(indexedNodeLabels),
		NodeTypes:         make(map[string]*schedulerobjects.NodeType),
		totalResources:    schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)},
		Db:                db,
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
	w.Flush()
	return sb.String()
}

// TODO: Pass through contexts to support timeouts.
func (nodeDb *NodeDb) ScheduleMany(reqs []*schedulerobjects.PodRequirements) ([]*PodSchedulingReport, bool, error) {
	txn := nodeDb.Db.Txn(true)
	defer txn.Abort()

	// Attempt to schedule pods one by one in a transaction.
	// If any pod fails to schedule, abort the transaction and return.
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
			err = nodeDb.BindNodeToPod(txn, req, report.Node)
			if err != nil {
				return nil, false, err
			}
		} else {
			for _, report := range reports {
				report.Node = nil
			}
			return reports, false, nil
		}
	}

	// All pods could be scheduled; commit the transaction and return.
	txn.Commit()
	return reports, true, nil
}

func (nodeDb *NodeDb) SelectAndBindNodeToPod(req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	txn := nodeDb.Db.Txn(true)
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
		err = nodeDb.BindNodeToPod(txn, req, report.Node)
		if err != nil {
			return nil, err
		}
	}
	return report, nil
}

func (nodeDb *NodeDb) SelectNodeForPod(req *schedulerobjects.PodRequirements) (*PodSchedulingReport, error) {
	return nodeDb.SelectNodeForPodWithTxn(nodeDb.Db.Txn(false), req)
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

func (nodeDb *NodeDb) BindNodeToPod(txn *memdb.Txn, req *schedulerobjects.PodRequirements, node *schedulerobjects.Node) error {
	// DeepCopy the node.
	// TODO: Use more efficient deepcopy.
	buffer, err := node.Marshal()
	if err != nil {
		return err
	}
	node = &schedulerobjects.Node{}
	err = proto.Unmarshal(buffer, node)
	if err != nil {
		return err
	}

	// Mark resources as allocated.
	schedulerobjects.AllocatableByPriorityAndResourceType(node.AllocatableByPriorityAndResource).MarkAllocated(
		req.Priority,
		schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests),
	)

	// Insert the node.
	err = txn.Insert("nodes", node)
	if err != nil {
		return errors.WithStack(err)
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
		available, ok := nodeDb.totalResources.Resources[string(t)]
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
		nodeDb.mu.Lock()
		nodeDb.NodeTypes[nodeType.Id] = nodeType
		nodeDb.mu.Unlock()

		// If this is a new node, increase the overall resource count.
		it, err := nodeDb.Db.Txn(false).Get("nodes", "id", node.Id)
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
