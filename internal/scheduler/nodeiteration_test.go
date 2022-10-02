package scheduler

import (
	"bytes"
	"container/heap"
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var testPriorities []int32 = []int32{0, 1, 2, 3}
var testResources []string = []string{"cpu", "memory", "gpu"}

func TestSchema(t *testing.T) {
	err := nodeDbSchema(testPriorities, testResources).Validate()
	assert.NoError(t, err)
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
		// TODO: These two cases fail. Do we need to support them?
		// "1 1001m": {
		// 	A: resource.MustParse("1"),
		// 	B: resource.MustParse("1001m"),
		// },
		// "1 1000m": {
		// 	A: resource.MustParse("1"),
		// 	B: resource.MustParse("1000m"),
		// },
		"1Gi 1001Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1001Mi"),
		},
		"1Gi 1000Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1000Mi"),
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
			}},
		"4 10 11": {
			expected: []*NodeTypesResourceIteratorItem{
				{priority: resource.MustParse("4")},
				{priority: resource.MustParse("10")},
				{priority: resource.MustParse("11")},
			}},
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
		Items                  []*SchedulerNode
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			NodeTypeId:    "foo",
			Resource:      "cpu",
			Priority:      1,
			Items:         testNodeItems1,
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			NodeTypeId:    "bar",
			Resource:      "cpu",
			Priority:      1,
			Items:         testNodeItems1,
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			NodeTypeId:             "foo",
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testNodeItems1,
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
				&NodeType{id: tc.NodeTypeId},
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
		Items                  []*SchedulerNode
		ExpectedOrder          []int
	}{
		"NodeType foo": {
			NodeTypes:     []string{"foo"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testNodeItems1,
			ExpectedOrder: []int{0, 1},
		},
		"NodeType bar": {
			NodeTypes:     []string{"bar"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testNodeItems1,
			ExpectedOrder: []int{2},
		},
		"NodeType foo, cpu lower bound": {
			NodeTypes:              []string{"foo"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testNodeItems1,
			ExpectedOrder:          []int{1},
		},
		"NodeType foo and bar": {
			NodeTypes:     []string{"foo", "bar"},
			Resource:      "cpu",
			Priority:      1,
			Items:         testNodeItems1,
			ExpectedOrder: []int{0, 1, 2},
		},
		"NodeType foo and bar, cpu lower bound": {
			NodeTypes:              []string{"foo", "bar"},
			Resource:               "cpu",
			Priority:               2,
			RequiredResourceAmount: resource.MustParse("6"),
			Items:                  testNodeItems1,
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

			nodeTypes := make([]*NodeType, len(tc.NodeTypes))
			for i, nodeTypeId := range tc.NodeTypes {
				nodeTypes[i] = &NodeType{id: nodeTypeId}
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

var testNodeItems1 []*SchedulerNode = []*SchedulerNode{
	{
		Id:         "node1",
		NodeTypeId: "foo",
		NodeType:   &NodeType{id: "foo"},
		AvailableResources: map[int32]map[string]resource.Quantity{
			0: {"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
			1: {"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")},
			2: {"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")},
		},
	},
	{
		Id:         "node2",
		NodeTypeId: "foo",
		NodeType:   &NodeType{id: "foo"},
		AvailableResources: map[int32]map[string]resource.Quantity{
			0: {"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")},
			1: {"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			2: {"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
		},
	},
	{
		Id:         "node3",
		NodeTypeId: "bar",
		NodeType:   &NodeType{id: "bar"},
		AvailableResources: map[int32]map[string]resource.Quantity{
			0: {"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi"), "gpu": resource.MustParse("1")},
			1: {"cpu": resource.MustParse("8"), "memory": resource.MustParse("8Gi"), "gpu": resource.MustParse("2")},
			2: {"cpu": resource.MustParse("9"), "memory": resource.MustParse("9Gi"), "gpu": resource.MustParse("3")},
		},
	},
}

// testNodeItems2 returns a randomly generated set of n nodes.
func testNodeItems2(priorities []int32, resources []string, n int) []*SchedulerNode {
	rv := make([]*SchedulerNode, n)
	for i := 0; i < n; i++ {
		rv[i] = &SchedulerNode{
			Id:                 uuid.NewString(),
			NodeTypeId:         "foo", // All nodes have the same node type.
			NodeType:           &NodeType{id: "bar"},
			AvailableResources: NewAvailableByPriorityAndResourceType(priorities, nil),
		}
		for _, p := range priorities {
			rs := make(map[string]resource.Quantity)
			for _, r := range resources {
				rs[r] = resource.MustParse(fmt.Sprintf("%d", rand.Intn(100)))
			}
			rv[i].AvailableResources.MarkAvailable(p, rs)
		}
	}
	return rv
}

func testNodes3(numCpuNodes, numTaintedCpuNodes, numGpuNodes int, priorities []int32) []*SchedulerNode {
	rv := make([]*SchedulerNode, 0)
	for i := 0; i < numCpuNodes; i++ {
		node := &SchedulerNode{
			Id: uuid.NewString(),
			NodeType: &NodeType{
				id: "cpu",
			},
			NodeTypeId: "cpu",
			AvailableResources: NewAvailableByPriorityAndResourceType(
				priorities,
				map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			),
		}
		rv = append(rv, node)
	}
	for i := 0; i < numTaintedCpuNodes; i++ {
		node := &SchedulerNode{
			Id: uuid.NewString(),
			NodeType: &NodeType{
				id: "taintedCpu",
				Taints: []v1.Taint{
					{
						Key:    "largeJobsOnly",
						Value:  "true",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
			NodeTypeId: "taintedCpu",
			AvailableResources: NewAvailableByPriorityAndResourceType(
				priorities,
				map[string]resource.Quantity{
					"cpu":    resource.MustParse("32"),
					"memory": resource.MustParse("256Gi"),
				},
			),
		}
		rv = append(rv, node)
	}
	for i := 0; i < numTaintedCpuNodes; i++ {
		node := &SchedulerNode{
			Id: uuid.NewString(),
			NodeType: &NodeType{
				id: "gpu",
				Taints: []v1.Taint{
					{
						Key:    "gpu",
						Value:  "true",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
			NodeTypeId: "gpu",
			AvailableResources: NewAvailableByPriorityAndResourceType(
				priorities,
				map[string]resource.Quantity{
					"cpu":    resource.MustParse("64"),
					"memory": resource.MustParse("1024Gi"),
					"gpu":    resource.MustParse("8"),
				},
			),
		}
		rv = append(rv, node)
	}
	return rv
}

func testNodesByTemplate(templates []*SchedulerNode, ns []int) []*SchedulerNode {
	if len(templates) != len(ns) {
		panic("templates and ns must be of equal length")
	}
	numNodes := 0
	for _, n := range ns {
		numNodes += n
	}
	rv := make([]*SchedulerNode, 0)
	for _, template := range templates {
		for i := 0; i < ns[i]; i++ {
			node := &SchedulerNode{
				Id:                 uuid.NewString(),
				LastSeen:           template.LastSeen,
				NodeType:           template.NodeType,
				NodeTypeId:         template.NodeTypeId,
				NodeInfo:           template.NodeInfo,
				AvailableResources: template.AvailableResources.DeepCopy(),
			}
			rv = append(rv, node)
		}
	}
	return rv
}

func populateDatabase(db *memdb.MemDB, items []*SchedulerNode) error {
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
