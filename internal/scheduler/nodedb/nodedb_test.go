package nodedb

import (
	"fmt"
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
	schema, _ := nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResourceNames)
	assert.NoError(t, schema.Validate())
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
	nodes := testfixtures.N32CpuNodes(2, testfixtures.TestPriorities)
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
	nodes = testfixtures.N8GpuNodes(3, testfixtures.TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))
}

func TestSelectNodeForPod_NodeIdLabel_Success(t *testing.T) {
	nodes := testfixtures.N32CpuNodes(2, testfixtures.TestPriorities)
	nodeId := nodes[1].Id
	require.NotEmpty(t, nodeId)
	db, err := createNodeDb(nodes)
	require.NoError(t, err)
	reqs := testfixtures.WithNodeSelectorPodReqs(
		map[string]string{schedulerconfig.NodeIdLabel: nodeId},
		testfixtures.N1CpuPodReqs("A", 0, 1),
	)
	for _, req := range reqs {
		pctx, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		require.NotNil(t, pctx.Node)
		assert.Equal(t, nodeId, pctx.Node.Id)
		assert.Equal(t, 0, len(pctx.NumExcludedNodesByReason))
		assert.Empty(t, pctx.NumExcludedNodesByReason)
	}
}

func TestSelectNodeForPod_NodeIdLabel_Failure(t *testing.T) {
	nodes := testfixtures.N32CpuNodes(1, testfixtures.TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := createNodeDb(nodes)
	require.NoError(t, err)
	reqs := testfixtures.WithNodeSelectorPodReqs(
		map[string]string{schedulerconfig.NodeIdLabel: "this node does not exist"},
		testfixtures.N1CpuPodReqs("A", 0, 1),
	)
	for _, req := range reqs {
		pctx, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		assert.Nil(t, pctx.Node)
		assert.Equal(t, 1, len(pctx.NumExcludedNodesByReason))
	}
}

func TestNodeBindingEvictionUnbinding(t *testing.T) {
	// We add evictedPriority here to simplify the resource accounting checks
	// below; it is always present after the first eviction.
	node := testfixtures.Test8GpuNode(append(testfixtures.TestPriorities, evictedPriority))
	job := testfixtures.Test1GpuJob("A", testfixtures.PriorityClass0)
	jobId := job.GetId()
	priority := testfixtures.TestPriorityClasses[job.GetPriorityClassName()].Priority
	schedulingInfo := job.GetRequirements(testfixtures.TestPriorityClasses)
	podRequirements := schedulingInfo.ObjectRequirements[0].GetPodRequirements()
	requests := schedulerobjects.ResourceListFromV1ResourceList(job.GetResourceRequirements().Requests)

	boundNode, err := BindPodToNode(podRequirements, node)
	require.NoError(t, err)

	unboundNode, err := UnbindPodFromNode(podRequirements, boundNode)
	require.NoError(t, err)

	unboundMultipleNode, err := UnbindPodsFromNode([]*schedulerobjects.PodRequirements{podRequirements}, boundNode)
	require.NoError(t, err)

	evictedNode := boundNode.DeepCopy()
	err = EvictJobFromNodeInPlace(testfixtures.TestPriorityClasses, job, evictedNode)
	require.NoError(t, err)

	evictedUnboundNode, err := UnbindPodFromNode(podRequirements, evictedNode)
	require.NoError(t, err)

	evictedBoundNode, err := BindPodToNode(podRequirements, evictedNode)
	require.NoError(t, err)

	err = EvictJobFromNodeInPlace(testfixtures.TestPriorityClasses, job, node)
	require.Error(t, err)

	_, err = UnbindPodFromNode(podRequirements, node)
	require.Error(t, err)

	_, err = BindPodToNode(podRequirements, boundNode)
	require.Error(t, err)

	err = EvictJobFromNodeInPlace(testfixtures.TestPriorityClasses, job, evictedNode)
	require.Error(t, err)

	assertNodeAccountingEqual(t, node, unboundNode)
	assertNodeAccountingEqual(t, node, evictedUnboundNode)
	assertNodeAccountingEqual(t, unboundNode, evictedUnboundNode)
	assertNodeAccountingEqual(t, boundNode, evictedBoundNode)
	assertNodeAccountingEqual(t, unboundNode, unboundMultipleNode)

	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{jobId: requests},
			boundNode.AllocatedByJobId,
		),
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{jobId: requests},
			evictedNode.AllocatedByJobId,
		),
	)

	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{"A": requests},
			boundNode.AllocatedByQueue,
		),
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			map[string]schedulerobjects.ResourceList{"A": requests},
			evictedNode.AllocatedByQueue,
		),
	)

	expectedAllocatable := boundNode.TotalResources.DeepCopy()
	expectedAllocatable.Sub(requests)
	assert.True(t, expectedAllocatable.Equal(boundNode.AllocatableByPriorityAndResource[priority]))

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
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.N1CpuPodReqs("A", 0, 32),
			ExpectSuccess: testfixtures.Repeat(true, 32),
		},
		"not all jobs fit": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.N1CpuPodReqs("A", 0, 33),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
		"unavailable resource": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          testfixtures.N1GpuPodReqs("A", 0, 1),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"unsupported resource": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs: testfixtures.WithRequestsPodReqs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"gibberish": resource.MustParse("1"),
					},
				},
				testfixtures.N1CpuPodReqs("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"preemption": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          append(append(testfixtures.N1CpuPodReqs("A", 0, 32), testfixtures.N1CpuPodReqs("A", 1, 32)...), testfixtures.N1CpuPodReqs("A", 0, 32)...),
			ExpectSuccess: append(testfixtures.Repeat(true, 64), testfixtures.Repeat(false, 32)...),
		},
		"taints/tolerations": {
			Nodes:         testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          append(append(testfixtures.N1CpuPodReqs("A", 0, 1), testfixtures.N1GpuPodReqs("A", 0, 1)...), testfixtures.N32CpuPodReqs("A", 0, 1)...),
			ExpectSuccess: []bool{false, false, true},
		},
		"node selector": {
			Nodes: append(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				)...,
			),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"key": "value",
				},
				testfixtures.N1CpuPodReqs("A", 0, 33),
			),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
		"node selector with mismatched value": {
			Nodes: testfixtures.WithLabelsNodes(
				map[string]string{
					"key": "value",
				},
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"key": "this is the wrong value",
				},
				testfixtures.N1CpuPodReqs("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"node selector with missing label": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs: testfixtures.WithNodeSelectorPodReqs(
				map[string]string{
					"this label does not exist": "value",
				},
				testfixtures.N1CpuPodReqs("A", 0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"node affinity": {
			Nodes: append(
				testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
				testfixtures.WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
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
				testfixtures.N1CpuPodReqs("A", 0, 33),
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
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testfixtures.N1CpuPodReqs("A", 0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testfixtures.N1CpuPodReqs("A", 0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				testfixtures.N1CpuPodReqs("A", 0, 32),
				testfixtures.N1CpuPodReqs("A", 0, 33),
				testfixtures.N1CpuPodReqs("A", 0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				append(testfixtures.N32CpuPodReqs("A", 0, 1), testfixtures.N1CpuPodReqs("A", 0, 32)...),
				testfixtures.N1CpuPodReqs("A", 0, 1),
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
	benchmarkUpsert(testfixtures.N32CpuNodes(1, testfixtures.TestPriorities), b)
}

func BenchmarkUpsert1000(b *testing.B) {
	benchmarkUpsert(testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities), b)
}

func BenchmarkUpsert100000(b *testing.B) {
	benchmarkUpsert(testfixtures.N32CpuNodes(100000, testfixtures.TestPriorities), b)
}

func benchmarkSelectAndBindNodeToPod(nodes []*schedulerobjects.Node, reqs []*schedulerobjects.PodRequirements, b *testing.B) {
	nodeDb, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
	)
	require.NoError(b, err)

	err = nodeDb.UpsertMany(nodes)
	require.NoError(b, err)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := nodeDb.Txn(true)
		for _, req := range reqs {
			_, err := nodeDb.SelectAndBindNodeToPodWithTxn(txn, req)
			require.NoError(b, err)
		}
		txn.Abort()
	}
}

func BenchmarkSelectAndBindNodeToPod10CpuNodes320SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 320),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10CpuNodes640SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 640),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes3200SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 3200),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes6400SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 6400),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes32000SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 32000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes64000SmallJobs(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		testfixtures.N1CpuPodReqs("A", 0, 64000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		),
		testfixtures.N1CpuPodReqs("A", 0, 100),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		),
		testfixtures.N1CpuPodReqs("A", 0, 1000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(10000, testfixtures.TestPriorities),
		),
		testfixtures.N1CpuPodReqs("A", 0, 10000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPodResourceConstrained(b *testing.B) {
	nodes := append(append(
		testfixtures.N32CpuNodes(500, testfixtures.TestPriorities),
		testfixtures.N8GpuNodes(1, testfixtures.TestPriorities)...),
		testfixtures.N32CpuNodes(499, testfixtures.TestPriorities)...,
	)
	benchmarkSelectAndBindNodeToPod(
		nodes,
		testfixtures.N1GpuPodReqs("A", 0, 1),
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

func BenchmarkNodeDbStringFromPodRequirementsNotMetReason(b *testing.B) {
	nodeDb := &NodeDb{
		podRequirementsNotMetReasonStringCache: make(map[uint64]string, 128),
	}
	reason := &schedulerobjects.UntoleratedTaint{
		Taint: v1.Taint{Key: randomString(100), Value: randomString(100), Effect: v1.TaintEffectNoSchedule},
	}
	nodeDb.stringFromPodRequirementsNotMetReason(reason)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		nodeDb.stringFromPodRequirementsNotMetReason(reason)
	}
}

func randomString(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += fmt.Sprint(i)
	}
	return s
}
