package nodedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNodesIterator(t *testing.T) {
	tests := map[string]struct {
		Nodes []*internaltypes.Node
	}{
		"1 node": {
			Nodes: testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
		},
		"0 nodes": {
			Nodes: testfixtures.ItN32CpuNodes(0, testfixtures.TestPriorities),
		},
		"3 nodes": {
			Nodes: testfixtures.ItN32CpuNodes(3, testfixtures.TestPriorities),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexById := make(map[string]int)
			for i, node := range tc.Nodes {
				indexById[node.GetId()] = i
			}
			nodeDb, err := itNewNodeDbWithNodes(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			it, err := NewNodesIterator(nodeDb.Txn(false))
			if !assert.NoError(t, err) {
				return
			}

			sortedNodes := slices.Clone(tc.Nodes)
			slices.SortFunc(sortedNodes, func(a, b *internaltypes.Node) int {
				if a.GetId() < b.GetId() {
					return -1
				} else if a.GetId() > b.GetId() {
					return 1
				} else {
					return 0
				}
			})
			expected := make([]int, len(sortedNodes))
			for i, node := range sortedNodes {
				expected[i] = indexById[node.GetId()]
			}

			actual := make([]int, 0)
			for node := it.NextNode(); node != nil; node = it.NextNode() {
				actual = append(actual, indexById[node.GetId()])
			}

			assert.Equal(t, expected, actual)
		})
	}
}

func TestNodeTypeIterator(t *testing.T) {
	nodeTypeA := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "a"})
	nodeTypeB := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "b"})

	tests := map[string]struct {
		nodes            []*internaltypes.Node
		nodeTypeId       uint64
		priority         int32
		resourceRequests internaltypes.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeA,
					testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeB,
					testfixtures.ItN32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeA,
					testfixtures.ItN32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeId:       nodeTypeA.GetId(),
			priority:         0,
			resourceRequests: testfixtures.TestResourceListFactory.MakeAllZero(),
			expected: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 5),
			),
		},
		"filter nodes with insufficient resources and return in increasing order": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       nodeTypeA.GetId(),
			priority:         0,
			resourceRequests: cpu("16"),
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       nodeTypeA.GetId(),
			priority:         1,
			resourceRequests: cpu("16"),
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "1Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "2Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "129Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "130Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "131Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "130Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "128Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "129Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       nodeTypeA.GetId(),
			priority:         0,
			resourceRequests: cpuMem("16", "128Gi"),
			expected:         []int{6, 1, 0},
		},
		"double-nested ordering": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("31", "1Gi"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMemGpu("31", "1Gi", "1"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMemGpu("31", "1Gi", "2"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMemGpu("31", "1Gi", "5"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("31", "2Gi"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMemGpu("31", "2Gi", "1"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("32", "514Gi"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("32", "512Gi"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("32", "513Gi"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("33"),
						testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeId:       nodeTypeA.GetId(),
			priority:         0,
			resourceRequests: cpuMemGpu("32", "512Gi", "4"),
			expected:         []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := newNodeDbWithNodes(nil)
			require.NoError(t, err)

			entries := make([]*internaltypes.Node, len(tc.nodes))
			for i, node := range tc.nodes {
				// Set monotonically increasing node IDs to ensure nodes appear in predictable order.
				newNodeId := fmt.Sprintf("%d", i)
				entry := testfixtures.ItWithIdNodes(newNodeId, []*internaltypes.Node{node})[0]

				nodeDb.AddNodeToDb(entry)
				entries[i] = entry
			}
			require.NoError(t, nodeDb.UpsertMany(entries))

			indexedResourceRequests := make([]int64, len(testfixtures.TestResources))

			assert.Nil(t, err)
			for i, resourceName := range nodeDb.indexedResources {
				indexedResourceRequests[i], err = tc.resourceRequests.GetByName(resourceName)
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
	const nodeTypeALabel = "a"
	const nodeTypeBLabel = "b"
	const nodeTypeCLabel = "c"
	const nodeTypeDLabel = "d"

	nodeTypeA := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "a"})
	nodeTypeB := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "b"})
	nodeTypeC := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "c"})
	nodeTypeD := labelsToNodeType(map[string]string{testfixtures.NodeTypeLabel: "d"})

	tests := map[string]struct {
		nodes            []*internaltypes.Node
		nodeTypeIds      []uint64
		priority         int32
		resourceRequests internaltypes.ResourceList
		expected         []int
	}{
		"only yield nodes of the right nodeType": {
			nodes: armadaslices.Concatenate(
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeA,
					testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeB,
					testfixtures.ItN32CpuNodes(2, testfixtures.TestPriorities),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeC,
					testfixtures.ItN32CpuNodes(3, testfixtures.TestPriorities),
				),
			),
			nodeTypeIds:      []uint64{nodeTypeA.GetId(), nodeTypeC.GetId()},
			priority:         0,
			resourceRequests: testfixtures.TestResourceListFactory.MakeAllZero(),
			expected: armadaslices.Concatenate(
				testfixtures.IntRange(0, 0),
				testfixtures.IntRange(3, 5),
			),
		},
		"filter nodes with insufficient resources and return in increasing order": {
			nodes: armadaslices.Concatenate(
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeA,
					testfixtures.ItWithUsedResourcesNodes(0,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeB,
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeC,
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeD,
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("14"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []uint64{nodeTypeA.GetId(), nodeTypeB.GetId(), nodeTypeC.GetId()},
			priority:         0,
			resourceRequests: cpu("16"),
			expected:         []int{1, 0},
		},
		"filter nodes with insufficient resources at priority and return in increasing order": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						1,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("15"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("16"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						2,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []uint64{nodeTypeA.GetId()},
			priority:         1,
			resourceRequests: cpu("16"),
			expected:         []int{4, 7, 3, 6, 0, 1, 2},
		},
		"nested ordering": {
			nodes: testfixtures.ItWithNodeTypeNodes(
				nodeTypeA,
				armadaslices.Concatenate(
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "1Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "2Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "129Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "130Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("15", "131Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "130Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "128Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpuMem("16", "129Gi"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
					testfixtures.ItWithUsedResourcesNodes(
						0,
						cpu("17"),
						testfixtures.ItN32CpuNodes(1, testfixtures.TestPriorities),
					),
				),
			),
			nodeTypeIds:      []uint64{nodeTypeA.GetId()},
			priority:         0,
			resourceRequests: cpuMem("16", "128Gi"),
			expected:         []int{6, 1, 0},
		},
		"double-nested ordering": {
			nodes: armadaslices.Concatenate(
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeA,
					armadaslices.Concatenate(
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMem("31", "1Gi"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMemGpu("31", "1Gi", "1"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMemGpu("31", "1Gi", "2"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMemGpu("31", "1Gi", "5"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeB,
					armadaslices.Concatenate(
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMem("31", "2Gi"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMemGpu("31", "2Gi", "1"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMem("32", "514Gi"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMem("32", "512Gi"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
				testfixtures.ItWithNodeTypeNodes(
					nodeTypeC,
					armadaslices.Concatenate(
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpuMem("32", "513Gi"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
						testfixtures.ItWithUsedResourcesNodes(
							0,
							cpu("33"),
							testfixtures.ItN8GpuNodes(1, testfixtures.TestPriorities),
						),
					),
				),
			),
			nodeTypeIds:      []uint64{nodeTypeA.GetId(), nodeTypeB.GetId(), nodeTypeC.GetId()},
			priority:         0,
			resourceRequests: cpuMemGpu("32", "512Gi", "4"),
			expected:         []int{7, 5, 4, 2, 1, 0},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := newNodeDbWithNodes(nil)
			require.NoError(t, err)

			entries := make([]*internaltypes.Node, len(tc.nodes))
			for i, node := range tc.nodes {
				// Set monotonically increasing node IDs to ensure nodes appear in predictable order.
				nodeId := fmt.Sprintf("%d", i)
				entry := testfixtures.ItWithIdNodes(nodeId, []*internaltypes.Node{node})[0]
				entry = testfixtures.ItWithIndexNode(uint64(i), entry)

				require.NoError(t, err)

				nodeDb.AddNodeToDb(entry)

				entries[i] = entry
			}
			require.NoError(t, nodeDb.UpsertMany(entries))

			rr := tc.resourceRequests

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

func withNodeTypeNodes(nodeTypeLabel string, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for _, node := range nodes {
		node.Labels[testfixtures.NodeTypeLabel] = nodeTypeLabel
	}
	return nodes
}

func nodeTypeLabelToNodeTypeId(nodeTypeLabel string) uint64 {
	return labelsToNodeTypeId(map[string]string{testfixtures.NodeTypeLabel: nodeTypeLabel})
}

func gpuNodeTypeLabelToNodeTypeId(nodeTypeLabel string) uint64 {
	return labelsToNodeTypeId(map[string]string{testfixtures.NodeTypeLabel: nodeTypeLabel, "gpu": "true"})
}

func labelsToNodeTypeId(labels map[string]string) uint64 {
	nodeType := internaltypes.NewNodeType(
		[]v1.Taint{},
		labels,
		util.StringListToSet(testfixtures.TestIndexedTaints),
		util.StringListToSet(testfixtures.TestIndexedNodeLabels),
	)
	return nodeType.GetId()
}

func labelsToNodeType(labels map[string]string) *internaltypes.NodeType {
	nodeType := internaltypes.NewNodeType(
		[]v1.Taint{},
		labels,
		util.StringListToSet(testfixtures.TestIndexedTaints),
		util.StringListToSet(testfixtures.TestIndexedNodeLabels),
	)
	return nodeType
}

func cpu(cpu string) internaltypes.ResourceList {
	return testfixtures.TestResourceListFactory.FromNodeProto(
		map[string]resource.Quantity{
			"cpu": resource.MustParse(cpu),
		},
	)
}

func cpuMem(cpu string, memory string) internaltypes.ResourceList {
	return testfixtures.TestResourceListFactory.FromNodeProto(
		map[string]resource.Quantity{
			"cpu":    resource.MustParse(cpu),
			"memory": resource.MustParse(memory),
		},
	)
}

func cpuMemGpu(cpu string, memory string, gpu string) internaltypes.ResourceList {
	return testfixtures.TestResourceListFactory.FromNodeProto(
		map[string]resource.Quantity{
			"cpu":            resource.MustParse(cpu),
			"memory":         resource.MustParse(memory),
			"nvidia.com/gpu": resource.MustParse(gpu),
		},
	)
}
