package nodedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
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
			nodeDb, err := newNodeDbWithNodes(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			it, err := NewNodesIterator(nodeDb.Txn(false))
			if !assert.NoError(t, err) {
				return
			}

			sortedNodes := slices.Clone(tc.Nodes)
			slices.SortFunc(sortedNodes, func(a, b *schedulerobjects.Node) int {
				if a.Id < b.Id {
					return -1
				} else if a.Id > b.Id {
					return 1
				} else {
					return 0
				}
			})
			expected := make([]int, len(sortedNodes))
			for i, node := range sortedNodes {
				expected[i] = indexById[node.Id]
			}

			actual := make([]int, 0)
			for node := it.NextNode(); node != nil; node = it.NextNode() {
				actual = append(actual, indexById[node.GetId()])
			}

			assert.Equal(t, expected, actual)
		})
	}
}

func TestNodePairIterator(t *testing.T) {
	nodes := testfixtures.TestCluster()
	for i, nodeId := range []string{"A", "B", "C"} {
		nodes[i].Id = nodeId
	}
	nodeDb, err := newNodeDbWithNodes(nodes)
	require.NoError(t, err)
	entries := make([]*internaltypes.Node, len(nodes))
	for i, node := range nodes {
		entry, err := nodeDb.GetNode(node.Id)
		require.NoError(t, err)
		entries[i] = entry
	}

	txn := nodeDb.Txn(true)
	require.NoError(t, txn.Delete("nodes", entries[2]))
	txn.Commit()
	txnA := nodeDb.Txn(false)

	txn = nodeDb.Txn(true)
	require.NoError(t, txn.Delete("nodes", entries[0]))
	require.NoError(t, txn.Insert("nodes", entries[2]))
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
			NodeA: entries[0],
			NodeB: nil,
		},
		{
			NodeA: entries[1],
			NodeB: entries[1],
		},
		{
			NodeA: nil,
			NodeB: entries[2],
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
				testfixtures.WithNodeTypeNodes(
					1,
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeNodes(
					2,
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
							"cpu":            resource.MustParse("31"),
							"memory":         resource.MustParse("1Gi"),
							"nvidia.com/gpu": resource.MustParse("1"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":            resource.MustParse("31"),
							"memory":         resource.MustParse("1Gi"),
							"nvidia.com/gpu": resource.MustParse("2"),
						}},
						testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":            resource.MustParse("31"),
							"memory":         resource.MustParse("1Gi"),
							"nvidia.com/gpu": resource.MustParse("5"),
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
							"cpu":            resource.MustParse("31"),
							"memory":         resource.MustParse("2Gi"),
							"nvidia.com/gpu": resource.MustParse("1"),
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
				"cpu":            resource.MustParse("32"),
				"memory":         resource.MustParse("512Gi"),
				"nvidia.com/gpu": resource.MustParse("4"),
			}},
			expected: []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := newNodeDbWithNodes(nil)
			require.NoError(t, err)

			entries := make([]*internaltypes.Node, len(tc.nodes))
			for i, node := range tc.nodes {
				// Set monotonically increasing node IDs to ensure nodes appear in predictable order.
				node.Id = fmt.Sprintf("%d", i)

				entry, err := nodeDb.create(node)
				require.NoError(t, err)

				// We can safely override NodeTypeId, because Keys is recomputed upon insertion.
				entry = testWithNodeTypeId(entry, node.NodeType.Id)

				entries[i] = entry
			}
			require.NoError(t, nodeDb.UpsertMany(entries))

			indexedResourceRequests := make([]int64, len(testfixtures.TestResources))
			rr, err := testfixtures.TestResourceListFactory.FromJobResourceListFailOnUnknown(schedulerobjects.V1ResourceListFromResourceList(tc.resourceRequests))
			assert.Nil(t, err)
			for i, resourceName := range nodeDb.indexedResources {
				indexedResourceRequests[i], err = rr.GetByName(resourceName)
				assert.Nil(t, err)
			}
			keyIndex := -1
			for i, p := range nodeDb.nodeDbPriorities {
				if p == tc.priority {
					keyIndex = i
				}
			}
			require.NotEqual(t, -1, keyIndex)
			it, err := NewNodeTypeIterator(
				nodeDb.Txn(false),
				tc.nodeTypeId,
				nodeIndexName(keyIndex),
				tc.priority,
				keyIndex,
				nodeDb.indexedResources,
				indexedResourceRequests,
				nodeDb.indexedResourceResolution,
			)
			require.NoError(t, err)

			expected := make([]string, len(tc.expected))
			for i, nodeId := range tc.expected {
				expected[i] = fmt.Sprintf("%d", nodeId)
			}
			actual := make([]string, 0)
			for {
				node, err := it.NextNode()
				require.NoError(t, err)
				if node == nil {
					break
				}
				actual = append(actual, node.GetId())
			}
			assert.Equal(t, expected, actual)

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
				testfixtures.WithNodeTypeNodes(
					1,
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeNodes(
					2,
					testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeNodes(
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
				testfixtures.WithNodeTypeNodes(
					1,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeNodes(
					2,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeNodes(
					3,
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
			nodes: testfixtures.WithNodeTypeNodes(
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
				testfixtures.WithNodeTypeNodes(
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
								"cpu":            resource.MustParse("31"),
								"memory":         resource.MustParse("1Gi"),
								"nvidia.com/gpu": resource.MustParse("1"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":            resource.MustParse("31"),
								"memory":         resource.MustParse("1Gi"),
								"nvidia.com/gpu": resource.MustParse("2"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":            resource.MustParse("31"),
								"memory":         resource.MustParse("1Gi"),
								"nvidia.com/gpu": resource.MustParse("5"),
							}},
							testfixtures.N8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
				testfixtures.WithNodeTypeNodes(
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
								"cpu":            resource.MustParse("31"),
								"memory":         resource.MustParse("2Gi"),
								"nvidia.com/gpu": resource.MustParse("1"),
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
				testfixtures.WithNodeTypeNodes(
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
				"cpu":            resource.MustParse("32"),
				"memory":         resource.MustParse("512Gi"),
				"nvidia.com/gpu": resource.MustParse("4"),
			}},
			expected: []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := newNodeDbWithNodes(nil)
			require.NoError(t, err)

			entries := make([]*internaltypes.Node, len(tc.nodes))
			for i, node := range tc.nodes {
				// Set monotonically increasing node IDs to ensure nodes appear in predictable order.
				node.Id = fmt.Sprintf("%d", i)

				entry, err := nodeDb.create(node)
				require.NoError(t, err)

				// We can safely override NodeTypeId, because Keys is recomputed upon insertion.
				entry = testWithNodeTypeId(entry, node.NodeType.Id)

				entries[i] = entry
			}
			require.NoError(t, nodeDb.UpsertMany(entries))

			rr, err := testfixtures.TestResourceListFactory.FromJobResourceListFailOnUnknown(schedulerobjects.V1ResourceListFromResourceList(tc.resourceRequests))
			assert.Nil(t, err)

			indexedResourceRequests := make([]int64, len(testfixtures.TestResources))
			for i, resourceName := range testfixtures.TestResourceNames {
				indexedResourceRequests[i], err = rr.GetByName(resourceName)
				assert.Nil(t, err)
			}
			it, err := NewNodeTypesIterator(
				nodeDb.Txn(false),
				tc.nodeTypeIds,
				nodeDb.indexNameByPriority[tc.priority],
				tc.priority,
				nodeDb.keyIndexByPriority[tc.priority],
				nodeDb.indexedResources,
				indexedResourceRequests,
				nodeDb.indexedResourceResolution,
			)
			require.NoError(t, err)

			expected := make([]string, len(tc.expected))
			for i, nodeId := range tc.expected {
				expected[i] = fmt.Sprintf("%d", nodeId)
			}
			actual := make([]string, 0)
			for {
				node, err := it.NextNode()
				require.NoError(t, err)
				if node == nil {
					break
				}
				actual = append(actual, node.GetId())
			}
			assert.Equal(t, expected, actual)

			// Calling next again should still return nil.
			node, err := it.NextNode()
			require.NoError(t, err)
			require.Nil(t, node)
		})
	}
}

func BenchmarkNodeTypeIterator(b *testing.B) {
	// Create nodes with varying amounts of CPU available.
	numNodes := 1000
	allocatedMilliCpus := []int64{
		1, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900,
		2, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900,
		3, 4, 5, 6, 7, 8, 9,
	}
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
	nodeDb, err := newNodeDbWithNodes(nodes)
	require.NoError(b, err)

	// Create iterator for 0 CPU required and an unfeasible memory request,
	// such that the iterator has to consider all nodes.
	indexedResourceRequests := make([]int64, len(nodeDb.indexedResources))
	oneTiB := resource.MustParse("1Ti")
	indexedResourceRequests[1] = oneTiB.ScaledValue(0)
	nodeTypeId := maps.Keys(nodeDb.nodeTypes)[0]
	var priority int32
	txn := nodeDb.Txn(false)
	defer txn.Abort()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		it, err := NewNodeTypeIterator(
			txn,
			nodeTypeId,
			nodeDb.indexNameByPriority[priority],
			priority,
			nodeDb.keyIndexByPriority[priority],
			nodeDb.indexedResources,
			indexedResourceRequests,
			nodeDb.indexedResourceResolution,
		)
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

func testWithNodeTypeId(node *internaltypes.Node, nodeTypeId uint64) *internaltypes.Node {
	return internaltypes.CreateNode(
		node.GetId(),
		nodeTypeId,
		node.GetIndex(),
		node.GetExecutor(),
		node.GetName(),
		node.GetTaints(),
		node.GetLabels(),
		node.TotalResources,
		node.AllocatableByPriority,
		node.AllocatedByQueue,
		node.AllocatedByJobId,
		node.EvictedJobRunIds,
		node.Keys,
	)
}
