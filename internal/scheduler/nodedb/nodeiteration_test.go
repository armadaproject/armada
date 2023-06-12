package nodedb

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
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

	nodeDb, err := createNodeDb(nil)
	require.NoError(t, err)
	for _, node := range nodes {
		node.NodeDbKeys = make([][]byte, len(nodeDb.prioritiesToTryAssigningAt))
		for i, p := range nodeDb.prioritiesToTryAssigningAt {
			node.NodeDbKeys[i] = nodeDb.nodeDbKeyFromNode(node.NodeDbKeys[i], node, p)
		}
	}

	txn := nodeDb.Txn(true)
	require.NoError(t, txn.Insert("nodes", nodes[0]))
	require.NoError(t, txn.Insert("nodes", nodes[1]))
	txn.Commit()
	txnA := nodeDb.Txn(false)

	txn = nodeDb.Txn(true)
	require.NoError(t, txn.Delete("nodes", nodes[0]))
	require.NoError(t, txn.Insert("nodes", nodes[2]))
	txn.Commit()
	txnB := nodeDb.Txn(false)

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
		nodeTypeId       uint64
		priority         int32
		resourceRequests schedulerobjects.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					1,
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					2,
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					1,
					testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeId:       1,
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{},
			expected: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 5),
			),
		},
		"filter nodes with insufficient resources and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeId:       1,
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeId:       1,
			priority:         1,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeId: 1,
			priority:   0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("128Gi"),
			}},
			expected: []int{6, 1, 0},
		},
		"double-nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeId: 1,
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
			nodeDb, err := createNodeDb(nil)
			require.NoError(t, err)

			// Set monotonically increaseing node ids to ensure nodes appear in predictable order.
			for i, node := range tc.nodes {
				node.Id = fmt.Sprintf("%d", i)
			}
			indexByNodeId := make(map[string]int)
			for i, node := range tc.nodes {
				indexByNodeId[node.Id] = i
			}

			// Compute the keys necessary to efficiently iterate over nodes
			// and populate the database. We do this manually instead of using nodeDb.Upsert to control the nodeTypeId.
			for _, node := range tc.nodes {
				node.NodeDbKeys = make([][]byte, len(nodeDb.prioritiesToTryAssigningAt))
				for i, p := range nodeDb.prioritiesToTryAssigningAt {
					node.NodeDbKeys[i] = nodeDb.nodeDbKeyFromNode(node.NodeDbKeys[i], node, p)
				}
			}
			require.NoError(t, populateDatabase(nodeDb.db, tc.nodes))

			// Create iterator.
			indexedResourceRequests := make([]resource.Quantity, len(testfixtures.TestResources))
			for i, t := range nodeDb.indexedResources {
				indexedResourceRequests[i] = tc.resourceRequests.Get(t)
			}
			keyIndex := -1
			for i, p := range nodeDb.prioritiesToTryAssigningAt {
				if p == tc.priority {
					keyIndex = i
				}
			}
			require.NotEqual(t, -1, keyIndex)
			it, err := NewNodeTypeIterator2(nodeDb.Txn(false), tc.nodeTypeId, nodeResourceIndexName2(keyIndex), tc.priority, testfixtures.TestResourceNames, indexedResourceRequests, testfixtures.TestIndexedResourceResolutionMillis)
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

			// Calling next should always return nil from now on.
			for i := 0; i < 100; i++ {
				node, err := it.NextNode()
				require.NoError(t, err)
				require.Nil(t, node)
			}
		})
	}
}

func TestNodeTypesIterator(t *testing.T) {
	tests := map[string]struct {
		nodes            []*schedulerobjects.Node
		nodeTypeIds      []uint64
		priority         int32
		resourceRequests schedulerobjects.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.WithNodeTypeIdNodes(
					1,
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					2,
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					3,
					testfixtures.N32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeIds:      []uint64{1, 3},
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
					1,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					2,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					3,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					4,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("14")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []uint64{1, 2, 3},
			priority:         0,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeIds:      []uint64{1},
			priority:         1,
			resourceRequests: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.WithNodeTypeIdNodes(
				1,
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
			nodeTypeIds: []uint64{1},
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
					1,
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
					2,
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
					3,
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
			nodeTypeIds: []uint64{1, 2, 3},
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
			nodeDb, err := createNodeDb(nil)
			require.NoError(t, err)

			// Set monotonically increaseing node ids to ensure nodes appear in predictable order.
			for i, node := range tc.nodes {
				node.Id = fmt.Sprintf("%d", i)
			}
			indexByNodeId := make(map[string]int)
			for i, node := range tc.nodes {
				indexByNodeId[node.Id] = i
			}

			// Compute the keys necessary to efficiently iterate over nodes
			// and populate the database. We do this manually instead of using nodeDb.Upsert to control the nodeTypeId.
			for _, node := range tc.nodes {
				node.NodeDbKeys = make([][]byte, len(nodeDb.prioritiesToTryAssigningAt))
				for i, p := range nodeDb.prioritiesToTryAssigningAt {
					node.NodeDbKeys[i] = nodeDb.nodeDbKeyFromNode(node.NodeDbKeys[i], node, p)
				}
			}
			require.NoError(t, populateDatabase(nodeDb.db, tc.nodes))

			indexedResourceRequests := make([]resource.Quantity, len(testfixtures.TestResources))
			for i, t := range testfixtures.TestResourceNames {
				indexedResourceRequests[i] = tc.resourceRequests.Get(t)
			}
			keyIndex := -1
			for i, p := range nodeDb.prioritiesToTryAssigningAt {
				if p == tc.priority {
					keyIndex = i
				}
			}
			it, err := NewNodeTypesIterator(nodeDb.Txn(false), tc.nodeTypeIds, keyIndex, tc.priority, testfixtures.TestResourceNames, indexedResourceRequests, testfixtures.TestIndexedResourceResolutionMillis)
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
	schema, _ := nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResourceNames)
	db, err := memdb.NewMemDB(schema)
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

func BenchmarkNodeTypeIterator(b *testing.B) {
	numNodes := 1000

	// Create nodes with varying amounts of CPU available.
	// allocatedMilliCpus := []int64{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000}
	allocatedMilliCpus := []int64{0, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900}
	nodes := testfixtures.N32CpuNodes(numNodes, testfixtures.TestPriorities)
	for i, node := range nodes {
		var q resource.Quantity
		q.SetMilli(allocatedMilliCpus[i%len(allocatedMilliCpus)])
		testfixtures.WithUsedResourcesNodes(
			testfixtures.TestPriorities[len(testfixtures.TestPriorities)-1],
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": q}},
			[]*schedulerobjects.Node{node},
		)
	}
	nodeDb, err := createNodeDb(nodes)
	require.NoError(b, err)

	// Create iterator for 0 CPU required and an unfeasible memory request
	// to benchmark how quickly jobs re rejected.
	indexedResourceRequests := make([]resource.Quantity, len(nodeDb.indexedResources))
	indexedResourceRequests[1] = resource.MustParse("1Ti")
	nodeTypeId := maps.Keys(nodeDb.nodeTypes)[0]
	var priority int32
	keyIndex := 0
	indexName := nodeResourceIndexName2(keyIndex)
	txn := nodeDb.Txn(false)
	defer txn.Abort()

	// defer profile.Start(profile.CPUProfile).Stop()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it, err := NewNodeTypeIterator2(txn, nodeTypeId, indexName, priority, nodeDb.indexedResources, indexedResourceRequests, testfixtures.TestIndexedResourceResolutionMillis)
		// it, err := NewNodeTypeIterator(txn, nodeTypeId, priority, nodeDb.indexedResources, indexedResourceRequests)
		require.NoError(b, err)
		for {
			node, err := it.NextNode()
			require.NoError(b, err)
			if node == nil {
				break
			}
		}
	}
}
