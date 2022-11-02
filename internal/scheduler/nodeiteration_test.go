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

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

var (
	testPriorityClasses = []configuration.PriorityClass{{0, nil}, {1, nil}, {2, nil}, {3, nil}}
	testPriorities      = []int32{0, 1, 2, 3}
	testResources       = []string{"cpu", "memory", "gpu"}
)

func TestNodeDbSchema(t *testing.T) {
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

var testNodeItems1 []*schedulerobjects.Node = []*schedulerobjects.Node{
	{
		Id:         "node1",
		NodeTypeId: "foo",
		NodeType:   &schedulerobjects.NodeType{Id: "foo"},
		AvailableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
			0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}},
			2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
		},
	},
	{
		Id:         "node2",
		NodeTypeId: "foo",
		NodeType:   &schedulerobjects.NodeType{Id: "foo"},
		AvailableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
			0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")}},
			1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")}},
			2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")}},
		},
	},
	{
		Id:         "node3",
		NodeTypeId: "bar",
		NodeType:   &schedulerobjects.NodeType{Id: "bar"},
		AvailableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
			0: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi")}},
			1: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("8"), "memory": resource.MustParse("8Gi")}},
			2: {Resources: map[string]resource.Quantity{"cpu": resource.MustParse("9"), "memory": resource.MustParse("9Gi")}},
		},
	},
}

// testNodeItems2 returns a randomly generated set of n nodes.
func testNodeItems2(priorities []int32, resources []string, n int) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = &schedulerobjects.Node{
			Id:         uuid.NewString(),
			NodeTypeId: "foo", // All nodes have the same node type.
			NodeType:   &schedulerobjects.NodeType{Id: "bar"},
		}
		availableByPriorityAndResource := schedulerobjects.NewAllocatableByPriorityAndResourceType(priorities, nil)
		for _, p := range priorities {
			rs := make(map[string]resource.Quantity)
			for _, r := range resources {
				rs[r] = resource.MustParse(fmt.Sprintf("%d", rand.Intn(100)))
			}
			availableByPriorityAndResource.MarkAllocatable(p, schedulerobjects.ResourceList{Resources: rs})
		}
		rv[i].AvailableByPriorityAndResource = availableByPriorityAndResource
	}
	return rv
}

func testNodes3(numCpuNodes, numTaintedCpuNodes, numGpuNodes int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, 0)
	for i := 0; i < numCpuNodes; i++ {
		rv = append(rv, testCpuNode(priorities))
	}
	for i := 0; i < numTaintedCpuNodes; i++ {
		rv = append(rv, testTaintedCpuNode(priorities))
	}
	for i := 0; i < numGpuNodes; i++ {
		rv = append(rv, testTaintedGpuNode(priorities))
	}
	return rv
}

func testNCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testCpuNode(priorities)
	}
	return rv
}

func testNTaintedCpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testTaintedCpuNode(priorities)
	}
	return rv
}

func testNGpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testGpuNode(priorities)
	}
	return rv
}

func testNTaintedtGpuNode(n int, priorities []int32) []*schedulerobjects.Node {
	rv := make([]*schedulerobjects.Node, n)
	for i := 0; i < n; i++ {
		rv[i] = testTaintedGpuNode(priorities)
	}
	return rv
}

// func testNA100Node(n int, priorities []int32) []*schedulerobjects.Node {
// 	rv := make([]*schedulerobjects.Node, n)
// 	for i := 0; i < n; i++ {
// 		rv[i] = testA100GpuNode(priorities)
// 	}
// 	return rv
// }

func testCpuNode(priorities []int32) *schedulerobjects.Node {
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		NodeType: &schedulerobjects.NodeType{
			Id: "cpu",
		},
		NodeTypeId: "cpu",
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AvailableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		),
	}
}

func testTaintedCpuNode(priorities []int32) *schedulerobjects.Node {
	taints := []v1.Taint{
		{
			Key:    "largeJobsOnly",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	labels := map[string]string{
		"largeJobsOnly": "true",
	}
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		NodeType: &schedulerobjects.NodeType{
			Id:     "taintedCpu",
			Taints: taints,
			Labels: labels,
		},
		Taints:     taints,
		Labels:     labels,
		NodeTypeId: "taintedCpu",
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AvailableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		),
	}
}

func testGpuNode(priorities []int32) *schedulerobjects.Node {
	labels := map[string]string{
		"gpu": "true",
	}
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		NodeType: &schedulerobjects.NodeType{
			Id:     "gpu",
			Labels: labels,
		},
		Labels:     labels,
		NodeTypeId: "gpu",
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		},
		AvailableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		),
	}
}

func testTaintedGpuNode(priorities []int32) *schedulerobjects.Node {
	taints := []v1.Taint{
		{
			Key:    "gpu",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	labels := map[string]string{
		"gpu": "true",
	}
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		NodeType: &schedulerobjects.NodeType{
			Id:     "gpu",
			Taints: taints,
			Labels: labels,
		},
		Taints:     taints,
		Labels:     labels,
		NodeTypeId: "gpu",
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		},
		AvailableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			priorities,
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("64"),
				"memory": resource.MustParse("1024Gi"),
				"gpu":    resource.MustParse("8"),
			},
		),
	}
}

// func testA100GpuNode(priorities []int32) *schedulerobjects.Node {
// 	return &schedulerobjects.Node{
// 		Id: uuid.NewString(),
// 		NodeType: &schedulerobjects.NodeType{
// 			Id: "a100",
// 			Taints: []v1.Taint{
// 				{
// 					Key:    "gpu",
// 					Value:  "true",
// 					Effect: v1.TaintEffectNoSchedule,
// 				},
// 			},
// 		},
// 		Labels:     map[string]string{"a100": "true"},
// 		NodeTypeId: "a100",
// 		TotalResources: schedulerobjects.ResourceList{
// 			Resources: map[string]resource.Quantity{
// 				"cpu":    resource.MustParse("64"),
// 				"memory": resource.MustParse("1024Gi"),
// 				"gpu":    resource.MustParse("8"),
// 			},
// 		},
// 		AvailableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
// 			priorities,
// 			map[string]resource.Quantity{
// 				"cpu":    resource.MustParse("64"),
// 				"memory": resource.MustParse("1024Gi"),
// 				"gpu":    resource.MustParse("8"),
// 			},
// 		),
// 	}
// }

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
