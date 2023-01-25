package scheduler

import (
	"bytes"
	"container/heap"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodesIterator(t *testing.T) {
	tests := map[string]struct {
		Nodes []*schedulerobjects.Node
	}{
		"1 node": {
			Nodes: testNCpuNode(1, testPriorities),
		},
		"0 nodes": {
			Nodes: testNCpuNode(0, testPriorities),
		},
		"3 nodes": {
			Nodes: testNCpuNode(3, testPriorities),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			it, err := NewNodesIterator(nodeDb.Txn(false))
			if !assert.NoError(t, err) {
				return
			}

			expected := slices.Clone(tc.Nodes)
			slices.SortFunc(expected, func(a, b *schedulerobjects.Node) bool { return a.Id < b.Id })

			actual := make([]*schedulerobjects.Node, 0)
			for node := it.NextNode(); node != nil; node = it.NextNode() {
				actual = append(actual, node)
			}
			slices.SortFunc(actual, func(a, b *schedulerobjects.Node) bool { return a.Id < b.Id })

			assert.Equal(t, expected, actual)
		})
	}
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
		NodeTypeId             string
		Resource               string
		Priority               int32
		RequiredResourceAmount resource.Quantity
		Items                  []*schedulerobjects.Node
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			NodeTypeId:    "foo",
			Resource:      "cpu",
			Priority:      1,
			Items:         testCluster(),
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			NodeTypeId:    "bar",
			Resource:      "cpu",
			Priority:      1,
			Items:         testCluster(),
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testCluster(),
			ExpectedOrder:          []int{1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := memdb.NewMemDB(nodeDbSchema(testPriorities, testResources))
			if !assert.NoError(t, err) {
				return
			}
			err = populateDatabase(db, tc.Items)
			if !assert.NoError(t, err) {
				return
			}

			txn := db.Txn(false)
			it, err := NewNodeTypeResourceIterator(
				txn,
				tc.Resource,
				tc.Priority,
				&schedulerobjects.NodeType{Id: tc.NodeTypeId},
				tc.RequiredResourceAmount,
			)
			if !assert.NoError(t, err) {
				return
			}
			for _, i := range tc.ExpectedOrder {
				item := it.NextNodeItem()
				if !assert.Equal(t, tc.Items[i], item) {
					return
				}
			}
			item := it.NextNodeItem()
			if !assert.Nil(t, item) {
				return
			}
		})
	}
}

func TestNodeTypesResourceIterator(t *testing.T) {
	tests := map[string]struct {
		NodeTypes              []string
		Resource               string
		Priority               int32
		RequiredResourceAmount resource.Quantity
		Items                  []*schedulerobjects.Node
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			NodeTypes:     []string{"foo"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testCluster(),
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			NodeTypes:     []string{"bar"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testCluster(),
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			NodeTypes:              []string{"foo"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testCluster(),
			ExpectedOrder:          []int{1},
		},
		"NodeType foo and bar": {
			NodeTypes:     []string{"foo", "bar"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testCluster(),
			ExpectedOrder: []int{0, 1, 2},
		},
		"NodeType foo and bar, cpu lower bound": {
			NodeTypes:              []string{"foo", "bar"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testCluster(),
			ExpectedOrder:          []int{1, 2},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := memdb.NewMemDB(nodeDbSchema(testPriorities, testResources))
			if !assert.NoError(t, err) {
				return
			}
			err = populateDatabase(db, tc.Items)
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
				tc.Resource,
				tc.Priority,
				nodeTypes,
				tc.RequiredResourceAmount,
			)
			if !assert.NoError(t, err) {
				return
			}
			for _, i := range tc.ExpectedOrder {
				item := it.NextNodeItem()
				if !assert.Equal(t, tc.Items[i], item) {
					return
				}
			}
			item := it.NextNodeItem()
			if !assert.Nil(t, item) {
				return
			}
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
