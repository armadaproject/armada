package nodedb

import (
	"bytes"
	"container/heap"
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
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

// NodeIndex is an index for schedulerobjects.Node that returns node.NodeDbKeys[KeyIndex].
type NodeIndex struct {
	KeyIndex int
}

// FromArgs computes the index key from a set of arguments.
// Takes a single argument resourceAmount of type []byte.
func (index *NodeIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("must provide exactly one argument")
	}
	return args[0].([]byte), nil
}

// FromObject extracts the index key from a *schedulerobjects.Node.
func (index *NodeIndex) FromObject(raw interface{}) (bool, []byte, error) {
	node := raw.(*schedulerobjects.Node)
	return true, node.NodeDbKeys[index.KeyIndex], nil
}

// NodeTypesIterator is an iterator over all nodes of the given nodeTypes
// with at least some specified amount of resources allocatable at a given priority.
// For example, all nodes of nodeType "foo" and "bar" with at least 2 cores and 1Gi memory allocatable at priority 2.
// Nodes are returned in sorted order, from least to most of the specified resource available.
type NodeTypesIterator struct {
	pq *nodeTypesIteratorPQ
}

func NewNodeTypesIterator(
	txn *memdb.Txn,
	nodeTypeIds []uint64,
	indexName string,
	priority int32,
	indexedResources []string,
	indexedResourceRequests []resource.Quantity,
) (*NodeTypesIterator, error) {
	pq := &nodeTypesIteratorPQ{
		priority:         priority,
		indexedResources: indexedResources,
		items:            make([]*nodeTypesIteratorPQItem, 0, len(nodeTypeIds)),
	}
	for _, nodeTypeId := range nodeTypeIds {
		it, err := NewNodeTypeIterator(
			txn,
			nodeTypeId,
			indexName,
			priority,
			indexedResources,
			indexedResourceRequests,
		)
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
	return &NodeTypesIterator{pq: pq}, nil
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
		if cmp := qa.Cmp(qb); cmp == -1 {
			return true
		} else if cmp == 1 {
			return false
		}
	}
	// Tie-break by id.
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
	txn *memdb.Txn
	// Only yield nodes of this nodeType.
	nodeTypeId uint64
	// Priority at which to consider allocatable resources on the node.
	priority int32
	// Name of the memdb index used for node iteration.
	// Should correspond to the priority set for this iterator.
	indexName string
	// NodeDb indexed resources.
	indexedResources []string
	// Pod requests for indexed resources in the same order as indexedResources.
	indexedResourceRequests []resource.Quantity
	// Current lower bound on node allocatable resources looked for.
	// Updated in-place as the iterator makes progress.
	lowerBound []resource.Quantity
	// memdb key computed from nodeTypeId and lowerBound.
	// Stored here to avoid dynamic allocs.
	key []byte
	// Current iterator into the underlying memdb.
	// Updated in-place whenever lowerBound changes.
	memdbIterator  memdb.ResultIterator
	previousNodeId string
}

func NewNodeTypeIterator(
	txn *memdb.Txn,
	nodeTypeId uint64,
	indexName string,
	priority int32,
	indexedResources []string,
	indexedResourceRequests []resource.Quantity,
) (*NodeTypeIterator, error) {
	if len(indexedResources) != len(indexedResourceRequests) {
		return nil, errors.Errorf("indexedResources and resourceRequirements are not of equal length")
	}
	it := &NodeTypeIterator{
		txn:                     txn,
		nodeTypeId:              nodeTypeId,
		priority:                priority,
		indexName:               indexName,
		indexedResources:        indexedResources,
		indexedResourceRequests: indexedResourceRequests,
		lowerBound:              slices.Clone(indexedResourceRequests),
	}
	memdbIt, err := it.newNodeTypeIterator()
	if err != nil {
		return nil, err
	}
	it.memdbIterator = memdbIt
	return it, nil
}

func (it *NodeTypeIterator) newNodeTypeIterator() (memdb.ResultIterator, error) {
	it.key = it.key[0:0]
	it.key = appendNodeDbKey(it.key, it.nodeTypeId, it.lowerBound)
	memdbIt, err := it.txn.LowerBound(
		"nodes",
		it.indexName,
		it.key,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return memdbIt, nil
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
		if node.Id == it.previousNodeId {
			panic(fmt.Sprintf("iterator received the same node twice consecutively: %s", node.Id))
		}
		it.previousNodeId = node.Id
		if node.NodeTypeId != it.nodeTypeId {
			// There are no more nodes of this nodeType.
			return nil, nil
		}
		allocatableByPriority := node.AllocatableByPriorityAndResource[it.priority]
		if len(allocatableByPriority.Resources) == 0 {
			return nil, errors.Errorf("node %s has no resources registered at priority %d: %v", node.Id, it.priority, node.AllocatableByPriorityAndResource)
		}
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
				memdbIterator, err := it.newNodeTypeIterator()
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
