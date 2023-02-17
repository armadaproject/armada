package nodedb

import (
	"bytes"
	"container/heap"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodesIterator(t *testing.T) {
	tests := map[string]struct {
		Nodes []*schedulerobjects.Node
	}{
		"1 node": {
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
		},
		"0 nodes": {
			Nodes: testfixtures.TestNCpuNode(0, testfixtures.TestPriorities),
		},
		"3 nodes": {
			Nodes: testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexById := make(map[string]int)
			for i, node := range tc.Nodes {
				indexById[node.Id] = i
			}
			nodeDb, err := testfixtures.CreateNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			it, err := NewNodesIterator(nodeDb.Txn(false))
			if !assert.NoError(t, err) {
				return
			}

			sortedNodes := slices.Clone(tc.Nodes)
			slices.SortFunc(sortedNodes, func(a, b *schedulerobjects.Node) bool { return a.Id < b.Id })
			expected := make([]int, len(sortedNodes))
			for i, node := range sortedNodes {
				expected[i] = indexById[node.Id]
			}

			actual := make([]int, 0)
			for node := it.NextNode(); node != nil; node = it.NextNode() {
				actual = append(actual, indexById[node.Id])
			}

			assert.Equal(t, expected, actual)
		})
	}
}

func TestNodePairIterator(t *testing.T) {
	nodes := testfixtures.TestCluster()
	for i, c := range []string{"A", "B", "C"} {
		nodes[i].Id = c
	}

	db, err := memdb.NewMemDB(nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResources))
	require.NoError(t, err)

	txn := db.Txn(true)
	require.NoError(t, txn.Insert("nodes", nodes[0]))
	require.NoError(t, txn.Insert("nodes", nodes[1]))
	txn.Commit()
	txnA := db.Txn(false)

	txn = db.Txn(true)
	require.NoError(t, txn.Delete("nodes", nodes[0]))
	require.NoError(t, txn.Insert("nodes", nodes[2]))
	txn.Commit()
	txnB := db.Txn(false)

	it, err := NewNodePairIterator(txnA, txnB)
	require.NoError(t, err)

	actual := make([]*NodePairIteratorItem, 0)
	for item := it.NextItem(); item != nil; item = it.NextItem() {
		actual = append(actual, item)
	}
	expected := []*NodePairIteratorItem{
		{
			NodeA: nodes[0],
			NodeB: nil,
		},
		{
			NodeA: nodes[1],
			NodeB: nodes[1],
		},
		{
			NodeA: nil,
			NodeB: nodes[2],
		},
	}
	assert.Equal(t, expected, actual)
}

// The memdb internally uses bytes.Compare to compare keys.
// Here, we test that byte representation comparison of quantities works as expected.
func TestQuantityIndexComparison(t *testing.T) {
	tests := map[string]struct {
		A resource.Quantity
		B resource.Quantity
	}{
		"10Mi 10Mi": {
			A: resource.MustParse("10Mi"),
			B: resource.MustParse("10Mi"),
		},
		"5Mi 10Mi": {
			A: resource.MustParse("5Mi"),
			B: resource.MustParse("10Mi"),
		},
		"10Gi 10Gi": {
			A: resource.MustParse("10Gi"),
			B: resource.MustParse("10Gi"),
		},
		"5Gi 10Gi": {
			A: resource.MustParse("5Gi"),
			B: resource.MustParse("10Gi"),
		},
		"1 1": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1"),
		},
		"1 2": {
			A: resource.MustParse("1"),
			B: resource.MustParse("2"),
		},
		"100m 100m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("100M"),
		},
		"100m 200m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("200M"),
		},
		"54870m 54871m": {
			A: resource.MustParse("54870m"),
			B: resource.MustParse("54871m"),
		},
		"1000Ti 1001Ti": {
			A: resource.MustParse("1000Ti"),
			B: resource.MustParse("1001Ti"),
		},
		"1000Pi 1001Pi": {
			A: resource.MustParse("1000Pi"),
			B: resource.MustParse("1001Pi"),
		},
		"1 1001m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1001m"),
		},
		"1 1000m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1000m"),
		},
		"1Gi 1001Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1001Mi"),
		},
		"1Gi 1000Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1000Mi"),
		},
		"5188205838208Ki 5188205838209Ki": {
			A: resource.MustParse("5188205838208Ki"),
			B: resource.MustParse("5188205838209Ki"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			expected := tc.A.Cmp(tc.B)
			actual := bytes.Compare(encodeQuantity(tc.A), encodeQuantity(tc.B))
			assert.Equal(t, expected, actual)

			expected = tc.B.Cmp(tc.A)
			actual = bytes.Compare(encodeQuantity(tc.B), encodeQuantity(tc.A))
			assert.Equal(t, expected, actual)
		})
	}
}

func TestNodeTypesResourceIteratorPQ(t *testing.T) {
	tests := map[string]struct {
		expected []*NodeTypesResourceIteratorItem
	}{
		"1Mi 2Mi 3Mi": {
			expected: []*NodeTypesResourceIteratorItem{
				{priority: resource.MustParse("1Mi")},
				{priority: resource.MustParse("2Mi")},
				{priority: resource.MustParse("3Mi")},
			},
		},
		"4 10 11": {
			expected: []*NodeTypesResourceIteratorItem{
				{priority: resource.MustParse("4")},
				{priority: resource.MustParse("10")},
				{priority: resource.MustParse("11")},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pq := make(NodeTypesResourceIteratorPQ, 0)
			for _, v := range tc.expected {
				heap.Push(&pq, v)
			}
			assert.Equal(t, len(tc.expected), pq.Len())
			for i := range tc.expected {
				v := heap.Pop(&pq)
				actual, ok := v.(*NodeTypesResourceIteratorItem)
				if !assert.True(t, ok) {
					return
				}
				assert.Equal(t, tc.expected[i].priority, actual.priority)
			}
			assert.Equal(t, 0, len(pq))
		})
	}
}

func TestNodeTypeResourceIterator(t *testing.T) {
	tests := map[string]struct {
		DominantQueue          string
		MaxActiveQueues        int
		NodeTypeId             string
		Resource               string
		Priority               int32
		RequiredResourceAmount resource.Quantity
		Nodes                  []*schedulerobjects.Node
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			DominantQueue: "*",
			NodeTypeId:    "foo",
			Resource:      "cpu",
			Priority:      1,
			Nodes:         testfixtures.TestCluster(),
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			DominantQueue: "*",
			NodeTypeId:    "bar",
			Resource:      "cpu",
			Priority:      1,
			Nodes:         testfixtures.TestCluster(),
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			DominantQueue:          "*",
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Nodes:                  testfixtures.TestCluster(),
			ExpectedOrder:          []int{1},
		},
		"dominantQueue": {
			DominantQueue:          "A",
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
					1: append(
						testfixtures.TestNSmallCpuJob("A", 0, 3),
						testfixtures.TestNSmallCpuJob("B", 0, 3)...,
					),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{0, 1},
		},
		"dominantQueue and maxActiveQueues": {
			DominantQueue:          "A",
			MaxActiveQueues:        1,
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
					1: testfixtures.TestNSmallCpuJob("A", 0, 2),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{1},
		},
		"empty dominantQueue": {
			DominantQueue:          "",
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{1},
		},
		"empty dominantQueue and maxActiveQueues": {
			DominantQueue:          "",
			MaxActiveQueues:        0,
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := memdb.NewMemDB(nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResources))
			if !assert.NoError(t, err) {
				return
			}
			err = populateDatabase(db, tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			txn := db.Txn(false)
			it, err := NewNodeTypeResourceIterator(
				txn,
				tc.DominantQueue,
				tc.MaxActiveQueues,
				tc.Resource,
				tc.Priority,
				&schedulerobjects.NodeType{Id: tc.NodeTypeId},
				tc.RequiredResourceAmount,
			)
			if !assert.NoError(t, err) {
				return
			}

			indexById := make(map[string]int)
			for i, node := range tc.Nodes {
				indexById[node.Id] = i
			}
			actual := make([]int, 0, len(tc.ExpectedOrder))
			for node := it.NextNodeItem(); node != nil; node = it.NextNodeItem() {
				i, ok := indexById[node.Id]
				require.True(t, ok)
				actual = append(actual, i)
			}
			assert.Equal(t, tc.ExpectedOrder, actual)
		})
	}
}

func TestNodeTypesResourceIterator(t *testing.T) {
	tests := map[string]struct {
		DominantQueue          string
		MaxActiveQueues        int
		NodeTypes              []string
		Resource               string
		Priority               int32
		RequiredResourceAmount resource.Quantity
		Nodes                  []*schedulerobjects.Node
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			DominantQueue: "*",
			NodeTypes:     []string{"foo"},
			Resource:      "cpu",
			Priority:      1,
			Nodes:         testfixtures.TestCluster(),
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			DominantQueue: "*",
			NodeTypes:     []string{"bar"},
			Resource:      "cpu",
			Priority:      1,
			Nodes:         testfixtures.TestCluster(),
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			DominantQueue:          "*",
			NodeTypes:              []string{"foo"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Nodes:                  testfixtures.TestCluster(),
			ExpectedOrder:          []int{1},
		},
		"NodeType foo and bar": {
			DominantQueue: "*",
			NodeTypes:     []string{"foo", "bar"},
			Resource:      "cpu",
			Priority:      1,
			Nodes:         testfixtures.TestCluster(),
			ExpectedOrder: []int{0, 1, 2},
		},
		"NodeType foo and bar, cpu lower bound": {
			DominantQueue:          "*",
			NodeTypes:              []string{"foo", "bar"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Nodes:                  testfixtures.TestCluster(),
			ExpectedOrder:          []int{1, 2},
		},
		"dominantQueue": {
			DominantQueue:          "A",
			NodeTypes:              []string{"foo", "bar"},
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
					1: append(
						testfixtures.TestNSmallCpuJob("A", 0, 3),
						testfixtures.TestNSmallCpuJob("B", 0, 3)...,
					),
					2: append(
						testfixtures.TestNSmallCpuJob("A", 0, 1),
						testfixtures.TestNSmallCpuJob("B", 0, 2)...,
					),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{0, 1},
		},
		"dominantQueue and maxActiveQueues": {
			DominantQueue:          "A",
			MaxActiveQueues:        1,
			NodeTypes:              []string{"foo", "bar"},
			Resource:               "cpu",
			Priority:               1,
			RequiredResourceAmount: resource.MustParse("0"),
			Nodes: testfixtures.WithPodReqsNodes(
				map[int][]*schedulerobjects.PodRequirements{
					0: append(
						testfixtures.TestNSmallCpuJob("A", 0, 2),
						testfixtures.TestNSmallCpuJob("B", 0, 1)...,
					),
					1: testfixtures.TestNSmallCpuJob("A", 0, 3),
					2: append(
						testfixtures.TestNSmallCpuJob("A", 0, 1),
						testfixtures.TestNSmallCpuJob("B", 0, 2)...,
					),
				},
				testfixtures.TestCluster(),
			),
			ExpectedOrder: []int{1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := memdb.NewMemDB(nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResources))
			if !assert.NoError(t, err) {
				return
			}
			err = populateDatabase(db, tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}

			nodeTypes := make([]*schedulerobjects.NodeType, len(tc.NodeTypes))
			for i, nodeTypeId := range tc.NodeTypes {
				nodeTypes[i] = &schedulerobjects.NodeType{Id: nodeTypeId}
			}

			txn := db.Txn(false)
			it, err := NewNodeTypesResourceIterator(
				txn,
				tc.DominantQueue,
				tc.MaxActiveQueues,
				tc.Resource,
				tc.Priority,
				nodeTypes,
				tc.RequiredResourceAmount,
			)
			require.NoError(t, err)

			indexById := make(map[string]int)
			for i, node := range tc.Nodes {
				indexById[node.Id] = i
			}
			actual := make([]int, 0, len(tc.ExpectedOrder))
			for node := it.NextNodeItem(); node != nil; node = it.NextNodeItem() {
				i, ok := indexById[node.Id]
				require.True(t, ok)
				actual = append(actual, i)
			}
			assert.Equal(t, tc.ExpectedOrder, actual)
		})
	}
}

func populateDatabase(db *memdb.MemDB, items []*schedulerobjects.Node) error {
	txn := db.Txn(true)
	defer txn.Abort()
	for _, item := range items {
		err := txn.Insert("nodes", item)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	txn.Commit()
	return nil
}
