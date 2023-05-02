package nodedb

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

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
	size := 8
	buf := make([]byte, size)

	// This bit flips the sign bit on any sized signed twos-complement integer,
	// which when truncated to a uint of the same size will bias the value such
	// that the maximum negative int becomes 0, and the maximum positive int
	// becomes the maximum positive uint.
	scaled := val ^ int64(-1<<(size*8-1))

	binary.BigEndian.PutUint64(buf, uint64(scaled))
	return buf
}

// NodeTypesIterator is an iterator over all nodes of the given nodeTypes
// with at least some specified amount of resources allocatable at a given priority.
// For example, all nodes of nodeType "foo" and "bar" with at least 2 cores and 1Gi memory allocatable at priority 2.
// Nodes are returned in sorted order, from least to most of the specified resource available.
type NodeTypesIterator struct {
	priority                int32
	indexedResources        []string
	indexedResourceRequests []resource.Quantity
	pq                      *nodeTypesIteratorPQ
}

func NewNodeTypesIterator(txn *memdb.Txn, nodeTypeIds []string, priority int32, indexedResources []string, indexedResourceRequests []resource.Quantity) (*NodeTypesIterator, error) {
	pq := &nodeTypesIteratorPQ{
		priority:         priority,
		indexedResources: indexedResources,
		items:            make([]*nodeTypesIteratorPQItem, 0, len(nodeTypeIds)),
	}
	for _, nodeTypeId := range nodeTypeIds {
		it, err := NewNodeTypeIterator(txn, nodeTypeId, priority, indexedResources, indexedResourceRequests)
		if err != nil {
			return nil, err
		}
		node, err := it.NextNode()
		if err != nil {
			return nil, err
		}
		if node == nil {
			continue
		}
		heap.Push(pq, &nodeTypesIteratorPQItem{
			node: node,
			it:   it,
		})
	}
	return &NodeTypesIterator{
		priority:                priority,
		indexedResources:        indexedResources,
		indexedResourceRequests: indexedResourceRequests,
		pq:                      pq,
	}, nil
}

func (it *NodeTypesIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodeTypesIterator) Next() interface{} {
	v, err := it.NextNode()
	if err != nil {
		panic(err)
	}
	return v
}

func (it *NodeTypesIterator) NextNode() (*schedulerobjects.Node, error) {
	if it.pq.Len() == 0 {
		return nil, nil
	}
	pqItem := heap.Pop(it.pq).(*nodeTypesIteratorPQItem)
	node := pqItem.node
	nextNode, err := pqItem.it.NextNode()
	if err != nil {
		return nil, err
	}
	if nextNode != nil {
		pqItem.node = nextNode
		heap.Push(it.pq, pqItem)
	}
	return node, nil
}

type nodeTypesIteratorPQ struct {
	priority         int32
	indexedResources []string
	items            []*nodeTypesIteratorPQItem
}

type nodeTypesIteratorPQItem struct {
	node *schedulerobjects.Node
	it   *NodeTypeIterator
	// The index of the item in the heap. Maintained by the heap.Interface methods.
	index int
}

func (pq *nodeTypesIteratorPQ) Len() int { return len(pq.items) }

func (pq *nodeTypesIteratorPQ) Less(i, j int) bool {
	return pq.less(pq.items[i].node, pq.items[j].node)
}

func (it *nodeTypesIteratorPQ) less(a, b *schedulerobjects.Node) bool {
	allocatableByPriorityA := a.AllocatableByPriorityAndResource[it.priority]
	allocatableByPriorityB := b.AllocatableByPriorityAndResource[it.priority]
	for _, t := range it.indexedResources {
		qa := allocatableByPriorityA.Get(t)
		qb := allocatableByPriorityB.Get(t)
		cmp := qa.Cmp(qb)
		if cmp == -1 {
			return true
		} else if cmp == 1 {
			return false
		}
	}
	return a.Id < b.Id
}

func (pq *nodeTypesIteratorPQ) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *nodeTypesIteratorPQ) Push(x any) {
	n := len(pq.items)
	item := x.(*nodeTypesIteratorPQItem)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *nodeTypesIteratorPQ) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

// NodeTypeIterator is an iterator over all nodes of a given nodeType
// with at least some specified amount of resources allocatable at a given priority.
// For example, all nodes of nodeType "foo" with at least 2 cores and 1Gi memory allocatable at priority 2.
// Nodes are returned in sorted order, from least to most of the specified resource available.
type NodeTypeIterator struct {
	txn                     *memdb.Txn
	nodeTypeId              string
	priority                int32
	indexedResources        []string
	indexedResourceRequests []resource.Quantity
	lowerBound              []resource.Quantity
	memdbIterator           memdb.ResultIterator
}

func NewNodeTypeIterator(txn *memdb.Txn, nodeTypeId string, priority int32, indexedResources []string, indexedResourceRequests []resource.Quantity) (*NodeTypeIterator, error) {
	if len(indexedResources) != len(indexedResourceRequests) {
		return nil, errors.Errorf("indexedResources and resourceRequirements are not of equal length")
	}
	memdbIterator, err := newNodeTypeIterator(txn, nodeTypeId, indexedResourceRequests, priority)
	if err != nil {
		return nil, err
	}
	return &NodeTypeIterator{
		txn:                     txn,
		nodeTypeId:              nodeTypeId,
		priority:                priority,
		indexedResources:        indexedResources,
		indexedResourceRequests: indexedResourceRequests,
		lowerBound:              make([]resource.Quantity, len(indexedResourceRequests)),
		memdbIterator:           memdbIterator,
	}, nil
}

func newNodeTypeIterator(txn *memdb.Txn, nodeTypeId string, resourceRequirements []resource.Quantity, priority int32) (memdb.ResultIterator, error) {
	args := make([]interface{}, 2+len(resourceRequirements))
	args[0] = nodeTypeId
	for i, q := range resourceRequirements {
		args[i+1] = q
	}
	args[len(args)-1] = ""
	it, err := txn.LowerBound(
		"nodes",
		nodeResourceIndexName(priority),
		args...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return it, nil
}

func (it *NodeTypeIterator) WatchCh() <-chan struct{} {
	panic("not implemented")
}

func (it *NodeTypeIterator) Next() interface{} {
	v, err := it.NextNode()
	if err != nil {
		panic(err)
	}
	return v
}

func (it *NodeTypeIterator) NextNode() (*schedulerobjects.Node, error) {
	for {
		v := it.memdbIterator.Next()
		if v == nil {
			return nil, nil
		}
		node := v.(*schedulerobjects.Node)
		if it.nodeTypeId != "" && node.NodeTypeId != it.nodeTypeId {
			return nil, nil
		}
		allocatableByPriority := node.AllocatableByPriorityAndResource[it.priority]
		for i, t := range it.indexedResources {
			nodeQuantity := allocatableByPriority.Get(t)
			requestQuantity := it.indexedResourceRequests[i]
			it.lowerBound[i] = nodeQuantity

			// If nodeQuantity < requestQuantity, replace the iterator using the lowerBound.
			// If nodeQuantity >= requestQuantity for all resources, return the node.
			if nodeQuantity.Cmp(requestQuantity) == -1 {
				for j := i; j < len(it.indexedResources); j++ {
					it.lowerBound[j] = it.indexedResourceRequests[j]
				}
				memdbIterator, err := newNodeTypeIterator(it.txn, it.nodeTypeId, it.lowerBound, it.priority)
				if err != nil {
					return nil, err
				}
				it.memdbIterator = memdbIterator
				break
			} else if i == len(it.indexedResources)-1 {
				return node, nil
			}
		}
	}
}
