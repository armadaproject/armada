package nodedb

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNodesIterator(t *testing.T) {
	tests := map[string]struct {
		Nodes []*schedulerobjects.Node
	}{
		"1 node": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		},
		"0 nodes": {
			Nodes: testfixtures.N32CpuNodes(0, testfixtures.TestPriorities),
		},
		"3 nodes": {
			Nodes: testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexById := make(map[string]int)
			for i, node := range tc.Nodes {
				indexById[node.Id] = i
			}
			nodeDb, err := createNodeDb(tc.Nodes)
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

func TestNodeTypeIterator(t *testing.T) {
	tests := map[string]struct {
		nodes            []*schedulerobjects.Node
		nodeTypeId       string
		priority         int32
		resourceRequests schedulerobjects.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeId:       "foo",
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{},
			expected: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 5),
			),
		},
		"filter nodes with insufficient resources and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       "foo",
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       "foo",
			priority:         1,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("1Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("131Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("128Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("17"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId: "foo",
			priority:   0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("128Gi"),
			}},
			expected: []int{6, 1, 0},
		},
		"double-nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("1"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("2"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("5"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("2Gi"),
							"gpu":    resource.MustParse("1"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("514Gi"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("512Gi"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("513Gi"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("33"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId: "foo",
			priority:   0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("512Gi"),
				"gpu":    resource.MustParse("4"),
			}},
			expected: []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set monotonically increaseing node ids to ensure nodes appear in predictable order.
			for i, node := range tc.nodes {
				node.Id = fmt.Sprintf("%d", i)
			}

			indexByNodeId := make(map[string]int)
			for i, node := range tc.nodes {
				indexByNodeId[node.Id] = i
			}
			db, err := newTestNodeDb(tc.nodes)
			require.NoError(t, err)

			indexedResourceRequests := make([]resource.Quantity, len(testfixtures.TestResources))
			for i, t := range testfixtures.TestResources {
				indexedResourceRequests[i] = tc.resourceRequests.Get(t)
			}
			it, err := NewNodeTypeIterator(db.Txn(false), tc.nodeTypeId, tc.priority, testfixtures.TestResources, indexedResourceRequests)
			require.NoError(t, err)

			// Compare actual with expected order.
			actual := make([]int, 0)
			for {
				node, err := it.NextNode()
				require.NoError(t, err)
				if node == nil {
					break
				}
				i, ok := indexByNodeId[node.Id]
				require.True(t, ok)
				actual = append(actual, i)
			}
			assert.Equal(t, tc.expected, actual)

			// Calling next again should still return nil.
			node, err := it.NextNode()
			require.NoError(t, err)
			require.Nil(t, node)
		})
	}
}

func TestNodeTypesIterator(t *testing.T) {
	tests := map[string]struct {
		nodes            []*schedulerobjects.Node
		nodeTypeIds      []string
		priority         int32
		resourceRequests schedulerobjects.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"baz",
					testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeIds:      []string{"foo", "baz"},
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{},
			expected: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 5),
			),
		},
		"filter nodes with insufficient resources and return in increasing order": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"baz",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"foobar",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("14")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []string{"foo", "bar", "baz"},
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []string{"foo"},
			priority:         1,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				"foo",
				armadaslices.Concatenate(
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("1Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("131Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("128Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("17"),
						}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds: []string{"foo"},
			priority:    0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("128Gi"),
			}},
			expected: []int{6, 1, 0},
		},
		"double-nested ordering": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					armadaslices.Concatenate(
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("1"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("2"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("5"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					armadaslices.Concatenate(
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("2Gi"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("2Gi"),
								"gpu":    resource.MustParse("1"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("32"),
								"memory": resource.MustParse("514Gi"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("32"),
								"memory": resource.MustParse("512Gi"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"baz",
					armadaslices.Concatenate(
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("32"),
								"memory": resource.MustParse("513Gi"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu": resource.MustParse("33"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
			),
			nodeTypeIds: []string{"foo", "bar", "baz"},
			priority:    0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("512Gi"),
				"gpu":    resource.MustParse("4"),
			}},
			expected: []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set monotonically increaseing node ids to ensure nodes appear in predictable order.
			for i, node := range tc.nodes {
				node.Id = fmt.Sprintf("%d", i)
			}

			indexByNodeId := make(map[string]int)
			for i, node := range tc.nodes {
				indexByNodeId[node.Id] = i
			}
			db, err := newTestNodeDb(tc.nodes)
			require.NoError(t, err)

			indexedResourceRequests := make([]resource.Quantity, len(testfixtures.TestResources))
			for i, t := range testfixtures.TestResources {
				indexedResourceRequests[i] = tc.resourceRequests.Get(t)
			}
			it, err := NewNodeTypesIterator(db.Txn(false), tc.nodeTypeIds, tc.priority, testfixtures.TestResources, indexedResourceRequests)
			require.NoError(t, err)

			// Compare actual with expected order.
			actual := make([]int, 0)
			for {
				node, err := it.NextNode()
				require.NoError(t, err)
				if node == nil {
					break
				}
				i, ok := indexByNodeId[node.Id]
				require.True(t, ok)
				actual = append(actual, i)
			}
			assert.Equal(t, tc.expected, actual)

			// Calling next again should still return nil.
			node, err := it.NextNode()
			require.NoError(t, err)
			require.Nil(t, node)
		})
	}
}

func newTestNodeDb(nodes []*schedulerobjects.Node) (*memdb.MemDB, error) {
	db, err := memdb.NewMemDB(nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResources))
	if err != nil {
		return nil, err
	}
	err = populateDatabase(db, nodes)
	if err != nil {
		return nil, err
	}
	return db, nil
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
