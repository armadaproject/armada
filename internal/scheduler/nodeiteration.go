package scheduler

import (
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// NodesIterator is an iterator over all nodes in the db.
type NodesIterator struct {
	it memdb.ResultIterator
}

func NewNodesIterator(txn *memdb.Txn) (*NodesIterator, error) {
	it, err := txn.LowerBound("nodes", "id", "")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodesIterator{
		it: it,
	}, nil
}

func (it *NodesIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodesIterator) NextNode() *schedulerobjects.Node {
	obj := it.it.Next()
	if obj == nil {
		return nil
	}
	node, ok := obj.(*schedulerobjects.Node)
	if !ok {
		panic(fmt.Sprintf("expected *Node, but got %T", obj))
	}
	return node
}

func (it *NodesIterator) Next() interface{} {
	return it.NextNode()
}

// NodeTypesResourceIterator extends NodeTypeResourceIterator to iterate over nodes of several node types.
// Nodes are returned in sorted order, going from least to most of the specified resource available.
type NodeTypesResourceIterator struct {
	priority int32
	resource string
	pq       NodeTypesResourceIteratorPQ
}

func NewNodeTypesResourceIterator(txn *memdb.Txn, resource string, priority int32, nodeTypes []*schedulerobjects.NodeType, resourceQuantity resource.Quantity) (*NodeTypesResourceIterator, error) {
	pq := make(NodeTypesResourceIteratorPQ, 0, len(nodeTypes))
	for _, nodeType := range nodeTypes {
		it, err := NewNodeTypeResourceIterator(txn, resource, priority, nodeType, resourceQuantity)
		if err != nil {
			return nil, err
		}
		nodeItem := it.NextNodeItem()
		if nodeItem == nil {
			continue // No nodes with sufficient resources for this NodeType.
		}
		heap.Push(&pq, &NodeTypesResourceIteratorItem{
			value:    nodeItem,
			it:       it,
			priority: nodeItem.AvailableQuantityByPriorityAndResource(priority, resource),
		})
	}
	return &NodeTypesResourceIterator{
		priority: priority,
		resource: resource,
		pq:       pq,
	}, nil
}

func (it *NodeTypesResourceIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodeTypesResourceIterator) NextNodeItem() *schedulerobjects.Node {
	if it.pq.Len() == 0 {
		return nil
	}
	obj := heap.Pop(&it.pq)
	nodeTypesResourceIteratorItem, ok := obj.(*NodeTypesResourceIteratorItem)
	if !ok {
		panic(fmt.Sprintf("expected *NodeTypesResourceIteratorItem, but got %T", obj))
	}
	rv := nodeTypesResourceIteratorItem.value
	nodeItem := nodeTypesResourceIteratorItem.it.NextNodeItem()
	if nodeItem != nil {
		heap.Push(&it.pq, &NodeTypesResourceIteratorItem{
			value:    nodeItem,
			it:       nodeTypesResourceIteratorItem.it,
			priority: nodeItem.AvailableQuantityByPriorityAndResource(it.priority, it.resource),
		})
	}
	return rv
}

func (it *NodeTypesResourceIterator) Next() interface{} {
	return it.NextNodeItem()
}

// NodeTypesResourceIteratorPQ is a priority queue used by NodeTypesResourceIterator to return results
// from across several sub-iterators in order.
type NodeTypesResourceIteratorPQ []*NodeTypesResourceIteratorItem

type NodeTypesResourceIteratorItem struct {
	value *schedulerobjects.Node
	// The iterator that produced this value.
	it *NodeTypeResourceIterator
	// The priority of the item in the queue.
	priority resource.Quantity
	// The index of the item in the heap.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int
}

func (pq NodeTypesResourceIteratorPQ) Len() int { return len(pq) }

func (pq NodeTypesResourceIteratorPQ) Less(i, j int) bool {
	return pq[i].priority.Cmp(pq[j].priority) == -1
}

func (pq NodeTypesResourceIteratorPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *NodeTypesResourceIteratorPQ) Push(x any) {
	n := len(*pq)
	item := x.(*NodeTypesResourceIteratorItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *NodeTypesResourceIteratorPQ) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// NodeTypeResourceIterator is an iterator over all nodes of a given nodeType,
// for which there's at least some specified amount of a given resource available.
// For example, all nodes of type "foo" for which there's at least 1Gi of memory available.
//
// Available resources is the sum of unused resources and resources assigned to lower-priority jobs.
// Nodes are returned in sorted order, going from least to most of the specified resource available.
type NodeTypeResourceIterator struct {
	nodeType *schedulerobjects.NodeType
	it       memdb.ResultIterator
}

func (it *NodeTypeResourceIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodeTypeResourceIterator) NextNodeItem() *schedulerobjects.Node {
	obj := it.it.Next()
	if obj == nil {
		return nil
	}
	nodeItem, ok := obj.(*schedulerobjects.Node)
	if !ok {
		panic(fmt.Sprintf("expected *NodeItem, but got %T", obj))
	}
	if nodeItem.NodeTypeId != it.nodeType.Id {
		// The index is sorted by NodeType first.
		// So we've seen all nodes of this NodeType if this comparison fails.
		return nil
	}
	return nodeItem
}

func (it *NodeTypeResourceIterator) Next() interface{} {
	return it.NextNodeItem()
}

func NewNodeTypeResourceIterator(txn *memdb.Txn, resource string, priority int32, nodeType *schedulerobjects.NodeType, resourceAmount resource.Quantity) (*NodeTypeResourceIterator, error) {
	indexName := nodeResourcePriorityIndexName(resource, priority)
	it, err := txn.LowerBound("nodes", indexName, nodeType.Id, resourceAmount)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodeTypeResourceIterator{
		nodeType: nodeType,
		it:       it,
	}, nil
}

type NodeItemAvailableResourceIndex struct {
	// Resource name, e.g., "cpu", "gpu", or "memory".
	Resource string
	// Job priority.
	Priority int32
}

// FromArgs computes the index key from a set of arguments.
// Takes a single argument resourceAmount of type uint64.
func (s *NodeItemAvailableResourceIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	q, ok := args[0].(resource.Quantity)
	if !ok {
		return nil, errors.Errorf("expected Quantity, but got %T", args[0])
	}
	return encodeQuantity(q), nil
}

// FromObject extracts the index key from a *NodeItem object.
func (s *NodeItemAvailableResourceIndex) FromObject(raw interface{}) (bool, []byte, error) {
	nodeItem, ok := raw.(*schedulerobjects.Node)
	if !ok {
		return false, nil, errors.Errorf("expected *NodeItem, but got %T", raw)
	}
	q := nodeItem.AvailableQuantityByPriorityAndResource(s.Priority, s.Resource)
	return true, encodeQuantity(q), nil
}

func encodeQuantity(val resource.Quantity) []byte {
	// We assume that any quantity we want to compare can be represented as an int64.
	return encodeInt(val.MilliValue())
}

func encodeInt(val int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(val))
	return buf
}
