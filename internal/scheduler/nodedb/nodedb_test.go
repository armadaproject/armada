package nodedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNodeDbSchema(t *testing.T) {
	err := nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResources).Validate()
	assert.NoError(t, err)
}

// Test the accounting of total resources across all nodes.
func TestTotalResources(t *testing.T) {
	nodeDb, err := createNodeDb([]*schedulerobjects.Node{})
	if !assert.NoError(t, err) {
		return
	}
	expected := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting nodes for the first time should increase the resource count.
	nodes := testfixtures.TestNCpuNode(2, testfixtures.TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting the same nodes again should not affect total resource count.
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting new nodes should increase the resource count.
	nodes = testfixtures.TestNGpuNode(3, testfixtures.TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))
}

func TestSelectNodeForPod_TargetNodeIdAnnotation_Success(t *testing.T) {
	nodes := testfixtures.TestNCpuNode(1, testfixtures.TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := createNodeDb(nodes)
	require.NoError(t, err)
	reqs := testfixtures.WithAnnotationsPodReqs(
		map[string]string{schedulerconfig.TargetNodeIdAnnotation: nodeId},
		testfixtures.TestNSmallCpuJob("A", 0, 1),
	)
	for _, req := range reqs {
		report, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		require.NotNil(t, report.Node)
		assert.Equal(t, nodes[0].Id, report.Node.Id)
		assert.Equal(t, 0, len(report.NumExcludedNodesByReason))
	}
}

func TestSelectNodeForPod_TargetNodeIdAnnotation_Failure(t *testing.T) {
	nodes := testfixtures.TestNCpuNode(1, testfixtures.TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := createNodeDb(nodes)
	require.NoError(t, err)
	reqs := testfixtures.WithAnnotationsPodReqs(
		map[string]string{schedulerconfig.TargetNodeIdAnnotation: "this node does not exist"},
		testfixtures.TestNSmallCpuJob("A", 0, 1),
	)
	for _, req := range reqs {
		report, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		assert.Nil(t, report.Node)
		assert.Equal(t, 1, len(report.NumExcludedNodesByReason))
	}
}

func TestNodeBindingEvictionUnbinding(t *testing.T) {
	node := testfixtures.TestGpuNode(testfixtures.TestPriorities)
	req := testfixtures.TestNGpuJob("A", 0, 1)[0]
	request := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
	jobId, err := JobIdFromPodRequirements(req)
	require.NoError(t, err)

	boundNode, err := BindPodToNode(req, node)
	require.NoError(t, err)

	unboundNode, err := UnbindPodFromNode(req, boundNode)
	require.NoError(t, err)

	unboundMultipleNode, err := UnbindPodsFromNode([]*schedulerobjects.PodRequirements{req}, boundNode)
	require.NoError(t, err)

	evictedNode, err := EvictPodFromNode(req, boundNode)
	require.NoError(t, err)

	evictedUnboundNode, err := UnbindPodFromNode(req, evictedNode)
	require.NoError(t, err)

	evictedBoundNode, err := BindPodToNode(req, evictedNode)
	require.NoError(t, err)

	_, err = EvictPodFromNode(req, node)
	require.Error(t, err)

	_, err = UnbindPodFromNode(req, node)
	require.Error(t, err)

	_, err = BindPodToNode(req, boundNode)
	require.Error(t, err)

	_, err = EvictPodFromNode(req, evictedNode)
	require.Error(t, err)

	assertNodeAccountingEqual(t, node, unboundNode)
	assertNodeAccountingEqual(t, node, evictedUnboundNode)
	assertNodeAccountingEqual(t, unboundNode, evictedUnboundNode)
	assertNodeAccountingEqual(t, boundNode, evictedBoundNode)
	assertNodeAccountingEqual(t, unboundNode, unboundMultipleNode)

	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{jobId: request},
			boundNode.AllocatedByJobId,
		),
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{jobId: request},
			evictedNode.AllocatedByJobId,
		),
	)

	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{"A": request},
			boundNode.AllocatedByQueue,
		),
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{"A": request},
			evictedNode.AllocatedByQueue,
		),
	)

	expectedAllocatable := boundNode.TotalResources.DeepCopy()
	expectedAllocatable.Sub(request)
	assert.True(t, expectedAllocatable.Equal(boundNode.AllocatableByPriorityAndResource[req.Priority]))

	assert.Empty(t, unboundNode.AllocatedByJobId)
	assert.Empty(t, unboundNode.AllocatedByQueue)
	assert.Empty(t, unboundNode.EvictedJobRunIds)
}

func assertNodeAccountingEqual(t *testing.T, node1, node2 *schedulerobjects.Node) bool {
	rv := true
	rv = rv && assert.True(
		t,
		schedulerobjects.QuantityByPriorityAndResourceType(
			node1.AllocatableByPriorityAndResource,
		).Equal(
			schedulerobjects.QuantityByPriorityAndResourceType(
				node2.AllocatableByPriorityAndResource,
			),
		),
		"expected %v, but got %v",
		node1.AllocatableByPriorityAndResource,
		node2.AllocatableByPriorityAndResource,
	)
	rv = rv && assert.True(
		t,
		armadamaps.DeepEqual(
			node1.AllocatedByJobId,
			node2.AllocatedByJobId,
		),
		"expected %v, but got %v",
		node1.AllocatedByJobId,
		node2.AllocatedByJobId,
	)
	rv = rv && assert.True(
		t,
		armadamaps.DeepEqual(
			node1.AllocatedByQueue,
			node2.AllocatedByQueue,
		),
		"expected %v, but got %v",
		node1.AllocatedByQueue,
		node2.AllocatedByQueue,
	)
	rv = rv && assert.True(
		t,
		maps.Equal(
			node1.EvictedJobRunIds,
			node2.EvictedJobRunIds,
		),
		"expected %v, but got %v",
		node1.EvictedJobRunIds,
		node2.EvictedJobRunIds,
	)
	rv = rv && assert.True(
		t,
		armadamaps.DeepEqual(
			node1.NonArmadaAllocatedResources,
			node2.NonArmadaAllocatedResources,
		),
		"expected %v, but got %v",
		node1.NonArmadaAllocatedResources,
		node2.NonArmadaAllocatedResources,
	)
	return rv
}

func TestSelectAndBindNodeToPod(t *testing.T) {
	tests := map[string]struct {
		Nodes         []*schedulerobjects.Node
		Reqs          []*schedulerobjects.PodRequirements
		ExpectSuccess []bool
	}{
		"all jobs fit": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.TestNSmallCpuJob("A", 0, 32),
			ExpectSuccess: testfixtures.Repeat(true, 32),
		},
		"not all jobs fit": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.TestNSmallCpuJob("A", 0, 33),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
		"unavailable resource": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.TestNGpuJob("A", 0, 1),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"unsupported resource": {
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs: testfixtures.WithRequestsPodReqs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"gibberish": resource.MustParse("1"),
					},
				},
				testfixtures.TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"preemption": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          append(append(testfixtures.TestNSmallCpuJob("A", 0, 32), testfixtures.TestNSmallCpuJob("A", 1, 32)...), testfixtures.TestNSmallCpuJob("A", 0, 32)...),
			ExpectSuccess: append(testfixtures.Repeat(true, 64), testfixtures.Repeat(false, 32)...),
		},
		"taints/tolerations": {
			Nodes:         testfixtures.TestNTaintedCpuNode(1, testfixtures.TestPriorities),
			Reqs:          append(append(testfixtures.TestNSmallCpuJob("A", 0, 1), testfixtures.TestNGpuJob("A", 0, 1)...), testfixtures.TestNLargeCpuJob("A", 0, 1)...),
			ExpectSuccess: []bool{false, false, true},
		},
		"node selector": {
			Nodes: append(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				)...,
			),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"key": "value",
				},
				testfixtures.TestNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
		"node selector with mismatched value": {
			Nodes: testfixtures.WithLabelsNodes(
				map[string]string{
					"key": "value",
				},
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"key": "this is the wrong value",
				},
				testfixtures.TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"node selector with missing label": {
			Nodes: testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"this label does not exist": "value",
				},
				testfixtures.TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"node affinity": {
			Nodes: append(
				testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
				)...,
			),
			Reqs: testfixtures.WithNodeAffinityPodReqs(
				[]v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "key",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"value"},
							},
						},
					},
				},
				testfixtures.TestNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			require.NoError(t, err)
			for i, req := range tc.Reqs {
				report, err := nodeDb.SelectAndBindNodeToPod(req)
				require.NoError(t, err)
				if !tc.ExpectSuccess[i] {
					assert.Nil(t, report.Node)
					continue
				}
				assert.NotNil(t, report.Node)

				node, err := nodeDb.GetNode(report.Node.Id)
				require.NoError(t, err)
				jobId, err := JobIdFromPodRequirements(req)
				require.NoError(t, err)
				expected := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)
				actual, ok := node.AllocatedByJobId[jobId]
				require.True(t, ok)
				assert.True(t, actual.Equal(expected))
			}
		})
	}
}

func TestScheduleMany(t *testing.T) {
	tests := map[string]struct {
		// Nodes to schedule across.
		Nodes []*schedulerobjects.Node
		// Schedule one group of jobs at a time.
		// Each group is composed of a slice of pods.
		Reqs [][]*schedulerobjects.PodRequirements
		// For each group, whether we expect scheduling to succeed.
		ExpectSuccess []bool
	}{
		"simple success": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testfixtures.TestNSmallCpuJob("A", 0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testfixtures.TestNSmallCpuJob("A", 0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				testfixtures.TestNSmallCpuJob("A", 0, 32),
				testfixtures.TestNSmallCpuJob("A", 0, 33),
				testfixtures.TestNSmallCpuJob("A", 0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: testfixtures.TestNCpuNode(2, testfixtures.TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				append(testfixtures.TestNLargeCpuJob("A", 0, 1), testfixtures.TestNSmallCpuJob("A", 0, 32)...),
				testfixtures.TestNSmallCpuJob("A", 0, 1),
			},
			ExpectSuccess: []bool{true, false},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			for i, reqs := range tc.Reqs {
				reports, ok, err := nodeDb.ScheduleMany(reqs)
				if !assert.NoError(t, err) {
					return
				}
				if tc.ExpectSuccess[i] {
					assert.Equal(t, len(reqs), len(reports))
					for _, report := range reports {
						if !assert.NotNil(t, report.Node) {
							return
						}
					}
					assert.True(t, ok)
				} else {
					assert.False(t, ok)
				}
			}
		})
	}
}

func benchmarkUpsert(nodes []*schedulerobjects.Node, b *testing.B) {
	db, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := db.UpsertMany(nodes)
		if !assert.NoError(b, err) {
			return
		}
	}
}

func BenchmarkUpsert1(b *testing.B) {
	benchmarkUpsert(testfixtures.TestNCpuNode(1, testfixtures.TestPriorities), b)
}
func BenchmarkUpsert1000(b *testing.B) {
	benchmarkUpsert(testfixtures.TestNCpuNode(1000, testfixtures.TestPriorities), b)
}
func BenchmarkUpsert100000(b *testing.B) {
	benchmarkUpsert(testfixtures.TestNCpuNode(100000, testfixtures.TestPriorities), b)
}

func benchmarkSelectAndBindNodeToPod(nodes []*schedulerobjects.Node, reqs []*schedulerobjects.PodRequirements, b *testing.B) {
	db, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if !assert.NoError(b, err) {
		return
	}

	err = db.UpsertMany(nodes)
	if !assert.NoError(b, err) {
		return
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.db.Txn(true)
		for _, req := range reqs {
			_, err := db.SelectAndBindNodeToPodWithTxn(txn, req)
			if !assert.NoError(b, err) {
				return
			}
		}
		txn.Abort()
	}
}

func BenchmarkSelectAndBindNodeToPodOneCpuNode(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.TestNCpuNode(1, testfixtures.TestPriorities),
		testfixtures.TestNSmallCpuJob("A", 0, 32),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.TestNCpuNode(100, testfixtures.TestPriorities),
		testfixtures.TestNSmallCpuJob("A", 0, 320),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.TestNCpuNode(10000, testfixtures.TestPriorities),
		testfixtures.TestNSmallCpuJob("A", 0, 32000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.TestNCpuNode(100, testfixtures.TestPriorities),
		),
		testfixtures.TestNSmallCpuJob("A", 0, 100),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.TestNCpuNode(1000, testfixtures.TestPriorities),
		),
		testfixtures.TestNSmallCpuJob("A", 0, 1000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.TestNCpuNode(10000, testfixtures.TestPriorities),
		),
		testfixtures.TestNSmallCpuJob("A", 0, 10000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPodResourceConstrained(b *testing.B) {
	nodes := append(append(
		testfixtures.TestNCpuNode(500, testfixtures.TestPriorities),
		testfixtures.TestNGpuNode(1, testfixtures.TestPriorities)...),
		testfixtures.TestNCpuNode(499, testfixtures.TestPriorities)...,
	)
	benchmarkSelectAndBindNodeToPod(
		nodes,
		testfixtures.TestNGpuJob("A", 0, 1),
		b,
	)
}

func createNodeDb(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	db, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := db.UpsertMany(nodes); err != nil {
		return nil, err
	}
	return db, nil
}
