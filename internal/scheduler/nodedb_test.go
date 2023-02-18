package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodeDbSchema(t *testing.T) {
	err := nodeDbSchema(TestPriorities, TestResources).Validate()
	assert.NoError(t, err)
}

// Test the accounting of total resources across all nodes.
func TestTotalResources(t *testing.T) {
	nodeDb, err := CreateNodeDb([]*schedulerobjects.Node{})
	if !assert.NoError(t, err) {
		return
	}
	expected := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	assert.True(t, expected.Equal(nodeDb.TotalResources))

	// Upserting nodes for the first time should increase the resource count.
	nodes := TestNCpuNode(2, TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.TotalResources))

	// Upserting the same nodes again should not affect total resource count.
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.TotalResources))

	// Upserting new nodes should increase the resource count.
	nodes = TestNGpuNode(3, TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.UpsertMany(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.TotalResources))
}

func TestSelectNodeForPod_TargetNodeIdAnnotation_Success(t *testing.T) {
	nodes := TestNCpuNode(1, TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := CreateNodeDb(nodes)
	require.NoError(t, err)
	reqs := WithAnnotationsPodReqs(
		map[string]string{TargetNodeIdAnnotation: nodeId},
		TestNSmallCpuJob("A", 0, 1),
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
	nodes := TestNCpuNode(1, TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := CreateNodeDb(nodes)
	require.NoError(t, err)
	reqs := WithAnnotationsPodReqs(
		map[string]string{TargetNodeIdAnnotation: "this node does not exist"},
		TestNSmallCpuJob("A", 0, 1),
	)
	for _, req := range reqs {
		report, err := db.SelectNodeForPod(req)
		if !assert.NoError(t, err) {
			continue
		}
		assert.Nil(t, report.Node)
		assert.Equal(t, 0, len(report.NumExcludedNodesByReason))
	}
}

func TestPodToFromNodeBinding(t *testing.T) {
	node := TestGpuNode(TestPriorities)
	req := TestNGpuJob("A", 0, 1)[0]
	request := schedulerobjects.ResourceListFromV1ResourceList(req.ResourceRequirements.Requests)

	newNode, err := BindPodToNode(req, node)
	require.NoError(t, err)

	jobId, err := JobIdFromPodRequirements(req)
	require.NoError(t, err)
	assert.Equal(t, []string{jobId}, maps.Keys(newNode.AllocatedByJobId))
	assert.True(
		t,
		request.Equal(newNode.AllocatedByJobId[jobId]),
	)

	assert.Equal(t, []string{"A"}, maps.Keys(newNode.AllocatedByQueue))
	assert.True(
		t,
		request.Equal(newNode.AllocatedByQueue["A"]),
	)

	expectedAllocatable := newNode.TotalResources.DeepCopy()
	expectedAllocatable.Sub(request)
	assert.True(t, expectedAllocatable.Equal(newNode.AllocatableByPriorityAndResource[req.Priority]))

	newNode, err = UnbindPodFromNode(req, newNode)
	require.NoError(t, err)
	assert.Empty(t, newNode.AllocatedByJobId)
	assert.Empty(t, newNode.AllocatedByQueue)
	expectedAllocatable = newNode.TotalResources.DeepCopy()
	assert.True(t, expectedAllocatable.Equal(newNode.AllocatableByPriorityAndResource[req.Priority]))
}

func TestSelectAndBindNodeToPod(t *testing.T) {
	tests := map[string]struct {
		Nodes         []*schedulerobjects.Node
		Reqs          []*schedulerobjects.PodRequirements
		ExpectSuccess []bool
	}{
		"all jobs fit": {
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          TestNSmallCpuJob("A", 0, 32),
			ExpectSuccess: Repeat(true, 32),
		},
		"not all jobs fit": {
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          TestNSmallCpuJob("A", 0, 33),
			ExpectSuccess: append(Repeat(true, 32), Repeat(false, 1)...),
		},
		"unavailable resource": {
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          TestNGpuJob("A", 0, 1),
			ExpectSuccess: Repeat(false, 1),
		},
		"unsupported resource": {
			Nodes: TestNCpuNode(1, TestPriorities),
			Reqs: WithRequestsPodReqs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"gibberish": resource.MustParse("1"),
					},
				},
				TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: Repeat(false, 1),
		},
		"preemption": {
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          append(append(TestNSmallCpuJob("A", 0, 32), TestNSmallCpuJob("A", 1, 32)...), TestNSmallCpuJob("A", 0, 32)...),
			ExpectSuccess: append(Repeat(true, 64), Repeat(false, 32)...),
		},
		"taints/tolerations": {
			Nodes:         TestNTaintedCpuNode(1, TestPriorities),
			Reqs:          append(append(TestNSmallCpuJob("A", 0, 1), TestNGpuJob("A", 0, 1)...), TestNLargeCpuJob("A", 0, 1)...),
			ExpectSuccess: []bool{false, false, true},
		},
		"node selector": {
			Nodes: append(
				TestNCpuNode(1, TestPriorities),
				WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					TestNCpuNode(1, TestPriorities),
				)...,
			),
			Reqs: WithNodeSelectorPodReqs(
				map[string]string{
					"key": "value",
				},
				TestNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(Repeat(true, 32), Repeat(false, 1)...),
		},
		"node selector with mismatched value": {
			Nodes: WithLabelsNodes(
				map[string]string{
					"key": "value",
				},
				TestNCpuNode(1, TestPriorities),
			),
			Reqs: WithNodeSelectorPodReqs(
				map[string]string{
					"key": "this is the wrong value",
				},
				TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: Repeat(false, 1),
		},
		"node selector with missing label": {
			Nodes: TestNCpuNode(1, TestPriorities),
			Reqs: WithNodeSelectorPodReqs(
				map[string]string{
					"this label does not exist": "value",
				},
				TestNSmallCpuJob("A", 0, 1),
			),
			ExpectSuccess: Repeat(false, 1),
		},
		"node affinity": {
			Nodes: append(
				TestNCpuNode(1, TestPriorities),
				WithLabelsNodes(
					map[string]string{
						"key": "value",
					},
					TestNCpuNode(1, TestPriorities),
				)...,
			),
			Reqs: WithNodeAffinityPodReqs(
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
				TestNSmallCpuJob("A", 0, 33),
			),
			ExpectSuccess: append(Repeat(true, 32), Repeat(false, 1)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := CreateNodeDb(tc.Nodes)
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
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{TestNSmallCpuJob("A", 0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         TestNCpuNode(1, TestPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{TestNSmallCpuJob("A", 0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: TestNCpuNode(2, TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				TestNSmallCpuJob("A", 0, 32),
				TestNSmallCpuJob("A", 0, 33),
				TestNSmallCpuJob("A", 0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: TestNCpuNode(2, TestPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				append(TestNLargeCpuJob("A", 0, 1), TestNSmallCpuJob("A", 0, 32)...),
				TestNSmallCpuJob("A", 0, 1),
			},
			ExpectSuccess: []bool{true, false},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := CreateNodeDb(tc.Nodes)
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
		TestPriorityClasses,
		TestResources,
		TestIndexedTaints,
		TestIndexedNodeLabels,
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
	benchmarkUpsert(TestNCpuNode(1, TestPriorities), b)
}

func BenchmarkUpsert1000(b *testing.B) {
	benchmarkUpsert(TestNCpuNode(1000, TestPriorities), b)
}

func BenchmarkUpsert100000(b *testing.B) {
	benchmarkUpsert(TestNCpuNode(100000, TestPriorities), b)
}

func benchmarkSelectAndBindNodeToPod(nodes []*schedulerobjects.Node, reqs []*schedulerobjects.PodRequirements, b *testing.B) {
	db, err := NewNodeDb(
		TestPriorityClasses,
		TestResources,
		TestIndexedTaints,
		TestIndexedNodeLabels,
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
		txn := db.Db.Txn(true)
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
		TestNCpuNode(1, TestPriorities),
		TestNSmallCpuJob("A", 0, 32),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		TestNCpuNode(100, TestPriorities),
		TestNSmallCpuJob("A", 0, 320),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		TestNCpuNode(10000, TestPriorities),
		TestNSmallCpuJob("A", 0, 32000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			TestNCpuNode(100, TestPriorities),
		),
		TestNSmallCpuJob("A", 0, 100),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			TestNCpuNode(1000, TestPriorities),
		),
		TestNSmallCpuJob("A", 0, 1000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			TestNCpuNode(10000, TestPriorities),
		),
		TestNSmallCpuJob("A", 0, 10000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPodResourceConstrained(b *testing.B) {
	nodes := append(append(
		TestNCpuNode(500, TestPriorities),
		TestNGpuNode(1, TestPriorities)...),
		TestNCpuNode(499, TestPriorities)...,
	)
	benchmarkSelectAndBindNodeToPod(
		nodes,
		TestNGpuJob("A", 0, 1),
		b,
	)
}
