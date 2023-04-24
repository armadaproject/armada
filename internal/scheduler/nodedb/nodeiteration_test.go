package nodedb

import (
	"bytes"
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
		"-1 1": {
			A: resource.MustParse("-1"),
			B: resource.MustParse("1"),
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
					testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"foo",
					testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("131Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("128Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("17"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("1"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("2"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("1Gi"),
							"gpu":    resource.MustParse("5"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("31"),
							"memory": resource.MustParse("2Gi"),
							"gpu":    resource.MustParse("1"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("514Gi"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("512Gi"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("32"),
							"memory": resource.MustParse("513Gi"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("33"),
						}},
						testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
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
					testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
				),
				testfixtures.WithNodeTypeIdNodes(
					"baz",
					testfixtures.TestNCpuNode(3, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"bar",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"baz",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
				),
				testfixtures.WithNodeTypeIdNodes(
					"foobar",
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("14")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						1,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("15")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("16")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						2,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("17")}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("2Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("15"),
							"memory": resource.MustParse("131Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("130Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("128Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu":    resource.MustParse("16"),
							"memory": resource.MustParse("129Gi"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
					),
					testfixtures.WithUsedResourcesNodes(
						0,
						schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("17"),
						}},
						testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
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
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("1"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("2"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("1Gi"),
								"gpu":    resource.MustParse("5"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
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
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("31"),
								"memory": resource.MustParse("2Gi"),
								"gpu":    resource.MustParse("1"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("32"),
								"memory": resource.MustParse("514Gi"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu":    resource.MustParse("32"),
								"memory": resource.MustParse("512Gi"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
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
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
						),
						testfixtures.WithUsedResourcesNodes(
							0,
							schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{
								"cpu": resource.MustParse("33"),
							}},
							testfixtures.TestNGpuNode(1, testfixtures.TestPriorities),
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

func withPodReqsNodes(reqs map[int][]*schedulerobjects.PodRequirements, nodes []*schedulerobjects.Node) []*schedulerobjects.Node {
	for i := range nodes {
		for _, req := range reqs[i] {
			node, err := BindPodToNode(req, nodes[i])
			if err != nil {
				panic(err)
			}
			nodes[i] = node
		}
	}
	return nodes
}
