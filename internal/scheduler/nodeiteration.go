package scheduler

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type NodeIterator interface {
	NextNode() *schedulerobjects.Node
}

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

type NodePairIterator struct {
	itA   *NodesIterator
	itB   *NodesIterator
	nodeA *schedulerobjects.Node
	nodeB *schedulerobjects.Node
}

type NodePairIteratorItem struct {
	NodeA *schedulerobjects.Node
	NodeB *schedulerobjects.Node
}

func NewNodePairIterator(txnA, txnB *memdb.Txn) (*NodePairIterator, error) {
	itA, err := NewNodesIterator(txnA)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	itB, err := NewNodesIterator(txnB)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodePairIterator{
		itA: itA,
		itB: itB,
	}, nil
}

func (it *NodePairIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodePairIterator) NextItem() (rv *NodePairIteratorItem) {
	defer func() {
		if rv == nil {
			return
		}
		if rv.NodeA != nil {
			it.nodeA = nil
		}
		if rv.NodeB != nil {
			it.nodeB = nil
		}
	}()
	if it.nodeA == nil {
		it.nodeA = it.itA.NextNode()
	}
	if it.nodeB == nil {
		it.nodeB = it.itB.NextNode()
	}
	if it.nodeA == nil && it.nodeB == nil {
		return nil
	} else if it.nodeA == nil || it.nodeB == nil {
		return &NodePairIteratorItem{
			NodeA: it.nodeA,
			NodeB: it.nodeB,
		}
	}
	cmp := bytes.Compare([]byte(it.nodeA.Id), []byte(it.nodeB.Id))
	if cmp == 0 {
		return &NodePairIteratorItem{
			NodeA: it.nodeA,
			NodeB: it.nodeB,
		}
	} else if cmp == -1 {
		return &NodePairIteratorItem{
			NodeA: it.nodeA,
		}
	} else {
		return &NodePairIteratorItem{
			NodeB: it.nodeB,
		}
	}
}

func (it *NodePairIterator) Next() interface{} {
	return it.NextItem()
}

// NodeTypesResourceIterator extends NodeTypeResourceIterator to iterate over nodes of several node types.
// Nodes are returned in sorted order, going from least to most of the specified resource available.
//
// If exclusiveToQueue is "*", all nodes of the given node type are considered.
// Otherwise, only nodes exclusive to that queue are considered.
type NodeTypesResourceIterator struct {
	priority int32
	resource string
	pq       NodeTypesResourceIteratorPQ
}

func NewNodeTypesResourceIterator(txn *memdb.Txn, dominantQueue string, maxActiveQueues int, resource string, priority int32, nodeTypes []*schedulerobjects.NodeType, resourceQuantity resource.Quantity) (*NodeTypesResourceIterator, error) {
	pq := make(NodeTypesResourceIteratorPQ, 0, len(nodeTypes))
	for _, nodeType := range nodeTypes {
		it, err := NewNodeTypeResourceIterator(txn, dominantQueue, maxActiveQueues, resource, priority, nodeType, resourceQuantity)
		if err != nil {
			return nil, err
		}
		nodeItem := it.NextNodeItem()
		if nodeItem == nil {
			// No nodes with sufficient resources for this NodeType.
			continue
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
		// TODO: No need to allocate a new object here.
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
//
// If exclusiveToQueue is "*", all nodes of the given node type are considered.
// Otherwise, only nodes for which the given queue has the largest request are returned.
// If maxActiveQueues > 0, only nodes with less than or equal to this number of active queues are returned.
type NodeTypeResourceIterator struct {
	nodeType        *schedulerobjects.NodeType
	dominantQueue   string
	maxActiveQueues int
	it              memdb.ResultIterator
}

func (it *NodeTypeResourceIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodeTypeResourceIterator) NextNodeItem() *schedulerobjects.Node {
	obj := it.it.Next()
	if obj == nil {
		return nil
	}
	node, ok := obj.(*schedulerobjects.Node)
	if !ok {
		panic(fmt.Sprintf("expected *NodeItem, but got %T", obj))
	}
	// If it.dominantQueue != *, the index is sorted by node.DominantQueue() first.
	// Otherwise, the index is sorted by NodeTypeId first.
	if it.dominantQueue != "*" {
		if it.dominantQueue != node.DominantQueue() {
			return nil
		}
		if it.maxActiveQueues != 0 && node.NumActiveQueues() > it.maxActiveQueues {
			return nil
		}
	}
	if it.nodeType.Id != node.NodeTypeId {
		// We've seen all nodes of the specified node type.
		return nil
	}
	return node
}

func (it *NodeTypeResourceIterator) Next() interface{} {
	return it.NextNodeItem()
}

func NewNodeTypeResourceIterator(txn *memdb.Txn, dominantQueue string, maxActiveQueues int, resource string, priority int32, nodeType *schedulerobjects.NodeType, resourceAmount resource.Quantity) (*NodeTypeResourceIterator, error) {
	if dominantQueue == "*" && maxActiveQueues != 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "maxActiveQueues",
			Value:   maxActiveQueues,
			Message: "maxActiveQueues can only be used with non-wildcard dominantQueue",
		})
	}
	var it memdb.ResultIterator
	var err error
	if dominantQueue == "*" {
		indexName := nodeResourcePriorityIndexName(resource, priority)
		it, err = txn.LowerBound("nodes", indexName, nodeType.Id, resourceAmount)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		indexName := nodeDominantQueueResourcePriorityIndexName(resource, priority)
		it, err = txn.LowerBound(
			"nodes",
			indexName,
			encodeNodeDominantQueueIndexKey(dominantQueue, 0),
			nodeType.Id,
			resourceAmount,
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return &NodeTypeResourceIterator{
		nodeType:        nodeType,
		dominantQueue:   dominantQueue,
		maxActiveQueues: maxActiveQueues,
		it:              it,
	}, nil
}

type NodeAvailableResourceIndex struct {
	// Resource name, e.g., "cpu", "gpu", or "memory".
	Resource string
	// Job priority.
	Priority int32
}

// FromArgs computes the index key from a set of arguments.
// Takes a single argument resourceAmount of type resource.Quantity.
func (index *NodeAvailableResourceIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	q, ok := args[0].(resource.Quantity)
	if !ok {
		return nil, errors.Errorf("expected Quantity, but got %T", args[0])
	}
	return encodeQuantity(q), nil
}

// FromObject extracts the index key from a *schedulerobjects.Node.
func (index *NodeAvailableResourceIndex) FromObject(raw interface{}) (bool, []byte, error) {
	node, ok := raw.(*schedulerobjects.Node)
	if !ok {
		return false, nil, errors.Errorf("expected *Node, but got %T", raw)
	}
	q := node.AvailableQuantityByPriorityAndResource(index.Priority, index.Resource)
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

type NodeDominantQueueIndex struct{}

// FromArgs computes the index value from a set of arguments.
// Takes a single argument of type string.
func (index *NodeDominantQueueIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	b, ok := args[0].([]byte)
	if !ok {
		return nil, errors.Errorf("expected string, but got %T", args[0])
	}
	return b, nil
}

// FromObject extracts the index valuefrom a *schedulerobjects.Node object.
func (index *NodeDominantQueueIndex) FromObject(raw interface{}) (bool, []byte, error) {
	node, ok := raw.(*schedulerobjects.Node)
	if !ok {
		return false, nil, errors.Errorf("expected *Node, but got %T", raw)
	}
	return true, encodeNodeDominantQueueIndexKey(node.DominantQueue(), node.NumActiveQueues()), nil
}

func encodeNodeDominantQueueIndexKey(dominantQueue string, numActiveQueues int) []byte {
	return append([]byte(dominantQueue), encodeInt(int64(numActiveQueues))...)
}
