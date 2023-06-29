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
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
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
	jobs := testfixtures.WithNodeSelectorJobs(
		map[string]string{schedulerconfig.NodeIdLabel: nodeId},
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
	)
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
	for _, jctx := range jctxs {
		txn := db.Txn(false)
		err := db.SelectNodeForJobWithTxn(txn, jctx)
		txn.Abort()
		if !assert.NoError(t, err) {
			continue
		}
		pctx := jctx.PodSchedulingContext
		require.NotNil(t, pctx)
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
	jobs := testfixtures.WithNodeSelectorJobs(
		map[string]string{schedulerconfig.NodeIdLabel: "this node does not exist"},
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
	)
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
	for _, jctx := range jctxs {
		txn := db.Txn(false)
		err := db.SelectNodeForJobWithTxn(txn, jctx)
		txn.Abort()
		if !assert.NoError(t, err) {
			continue
		}
		pctx := jctx.PodSchedulingContext
		require.NotNil(t, pctx)
		assert.Nil(t, pctx.Node)
		assert.Equal(t, 1, len(pctx.NumExcludedNodesByReason))
	}
}

func TestNodeBindingEvictionUnbinding(t *testing.T) {
	jobFilter := func(job interfaces.LegacySchedulerJob) bool { return true }
	node := testfixtures.Test8GpuNode(append(testfixtures.TestPriorities, evictedPriority))
	job := testfixtures.Test1GpuJob("A", testfixtures.PriorityClass0)
	request := schedulerobjects.ResourceListFromV1ResourceList(job.GetResourceRequirements().Requests)
	jobId := job.GetId()

	boundNode, err := BindJobToNode(testfixtures.TestPriorityClasses, job, node)
	require.NoError(t, err)

	unboundNode, err := UnbindJobFromNode(testfixtures.TestPriorityClasses, job, boundNode)
	require.NoError(t, err)

	unboundMultipleNode, err := UnbindJobsFromNode(testfixtures.TestPriorityClasses, []interfaces.LegacySchedulerJob{job}, boundNode)
	require.NoError(t, err)

	evictedJobs, evictedNode, err := EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, boundNode)
	require.NoError(t, err)
	assert.Equal(t, []interfaces.LegacySchedulerJob{job}, evictedJobs)

	evictedUnboundNode, err := UnbindJobFromNode(testfixtures.TestPriorityClasses, job, evictedNode)
	require.NoError(t, err)

	evictedBoundNode, err := BindJobToNode(testfixtures.TestPriorityClasses, job, evictedNode)
	require.NoError(t, err)

	_, _, err = EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, node)
	require.Error(t, err)

	_, err = UnbindJobFromNode(testfixtures.TestPriorityClasses, job, node)
	require.Error(t, err)

	_, err = BindJobToNode(testfixtures.TestPriorityClasses, job, boundNode)
	require.Error(t, err)

	_, _, err = EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, evictedNode)
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
	priority := testfixtures.TestPriorityClasses[job.GetPriorityClassName()].Priority
	assert.True(t, expectedAllocatable.Equal(boundNode.AllocatableByPriorityAndResource[priority]))

	assert.Empty(t, unboundNode.AllocatedByJobId)
	assert.Empty(t, unboundNode.AllocatedByQueue)
	assert.Empty(t, unboundNode.EvictedJobRunIds)
}

func assertNodeAccountingEqual(t *testing.T, node1, node2 *schedulerobjects.Node) bool {
	rv := true
	rv = rv && assert.True(
		t,
		schedulerobjects.QuantityByTAndResourceType[int32](
			node1.AllocatableByPriorityAndResource,
		).Equal(
			schedulerobjects.QuantityByTAndResourceType[int32](
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

func TestEviction(t *testing.T) {
	tests := map[string]struct {
		jobFilter         func(interfaces.LegacySchedulerJob) bool
		expectedEvictions []int32
	}{
		"jobFilter always returns false": {
			jobFilter:         func(_ interfaces.LegacySchedulerJob) bool { return false },
			expectedEvictions: []int32{},
		},
		"jobFilter always returns true": {
			jobFilter:         func(_ interfaces.LegacySchedulerJob) bool { return true },
			expectedEvictions: []int32{0, 1},
		},
		"jobFilter returns true for preemptible jobs": {
			jobFilter: func(job interfaces.LegacySchedulerJob) bool {
				priorityClassName := job.GetPriorityClassName()
				priorityClass := testfixtures.TestPriorityClasses[priorityClassName]
				return priorityClass.Preemptible
			},
			expectedEvictions: []int32{0},
		},
		"jobFilter nil": {
			jobFilter:         nil,
			expectedEvictions: []int32{0, 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := testfixtures.Test32CpuNode(testfixtures.TestPriorities)
			jobs := []interfaces.LegacySchedulerJob{
				testfixtures.Test1Cpu4GiJob("queue-alice", testfixtures.PriorityClass0),
				testfixtures.Test1Cpu4GiJob("queue-alice", testfixtures.PriorityClass3),
			}
			var err error
			for _, job := range jobs {
				node, err = BindJobToNode(testfixtures.TestPriorityClasses, job, node)
				require.NoError(t, err)
			}

			actualEvictions, _, err := EvictJobsFromNode(testfixtures.TestPriorityClasses, tc.jobFilter, jobs, node)
			require.NoError(t, err)
			expectedEvictions := make([]interfaces.LegacySchedulerJob, 0, len(tc.expectedEvictions))
			for _, i := range tc.expectedEvictions {
				expectedEvictions = append(expectedEvictions, jobs[i])
			}
			assert.Equal(t, expectedEvictions, actualEvictions)
		})
	}
}

func TestScheduleIndividually(t *testing.T) {
	tests := map[string]struct {
		Nodes         []*schedulerobjects.Node
		Jobs          []*jobdb.Job
		ExpectSuccess []bool
	}{
		"all jobs fit": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			ExpectSuccess: testfixtures.Repeat(true, 32),
		},
		"not all jobs fit": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
		"unavailable resource": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"unsupported resource": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: testfixtures.WithRequestsJobs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"gibberish": resource.MustParse("1"),
					},
				},
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"preemption": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: append(
				append(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 32)...,
				),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)...,
			),
			ExpectSuccess: append(testfixtures.Repeat(true, 64), testfixtures.Repeat(false, 32)...),
		},
		"taints/tolerations": {
			Nodes: testfixtures.NTainted32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: append(
				append(
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
					testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1)...,
				),
				testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1)...,
			),
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
			Jobs: testfixtures.WithNodeSelectorJobs(
				map[string]string{
					"key": "value",
				},
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
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
			Jobs: testfixtures.WithNodeSelectorJobs(
				map[string]string{
					"key": "this is the wrong value",
				},
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			),
			ExpectSuccess: testfixtures.Repeat(false, 1),
		},
		"node selector with missing label": {
			Nodes: testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs: testfixtures.WithNodeSelectorJobs(
				map[string]string{
					"this label does not exist": "value",
				},
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
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
			Jobs: testfixtures.WithNodeAffinityJobs(
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
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			),
			ExpectSuccess: append(testfixtures.Repeat(true, 32), testfixtures.Repeat(false, 1)...),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			require.NoError(t, err)

			jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, tc.Jobs)

			for i, jctx := range jctxs {
				ok, err := nodeDb.ScheduleMany([]*schedulercontext.JobSchedulingContext{jctx})
				require.NoError(t, err)
				pctx := jctx.PodSchedulingContext

				if !tc.ExpectSuccess[i] {
					assert.False(t, ok)
					if pctx != nil {
						assert.Nil(t, pctx.Node)
					}
					continue
				}

				assert.True(t, ok)
				require.NotNil(t, pctx)

				node := pctx.Node
				if !tc.ExpectSuccess[i] {
					assert.Nil(t, node)
					continue
				}
				require.NotNil(t, node)

				job := jctx.Job
				expected := schedulerobjects.ResourceListFromV1ResourceList(job.GetResourceRequirements().Requests)
				actual, ok := node.AllocatedByJobId[job.GetId()]
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
		Jobs [][]*jobdb.Job
		// For each group, whether we expect scheduling to succeed.
		ExpectSuccess []bool
	}{
		"simple success": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          [][]*jobdb.Job{testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          [][]*jobdb.Job{testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: [][]*jobdb.Job{
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: [][]*jobdb.Job{
				append(
					testfixtures.N32Cpu256GiJobs("A", testfixtures.PriorityClass0, 1),
					testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32)...,
				),
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
			},
			ExpectSuccess: []bool{true, false},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			require.NoError(t, err)
			for i, jobs := range tc.Jobs {
				jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
				ok, err := nodeDb.ScheduleMany(jctxs)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectSuccess[i], ok)
				for _, jctx := range jctxs {
					pctx := jctx.PodSchedulingContext
					require.NotNil(t, pctx)
					if tc.ExpectSuccess[i] {
						assert.NotNil(t, pctx.Node)
					}
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

func benchmarkScheduleMany(b *testing.B, nodes []*schedulerobjects.Node, jobs []*jobdb.Job) {
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
		jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
		txn := nodeDb.Txn(true)
		_, err := nodeDb.ScheduleManyWithTxn(txn, jctxs)
		txn.Abort()
		require.NoError(b, err)
	}
}

func BenchmarkScheduleMany10CpuNodes320SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 320),
	)
}

func BenchmarkScheduleMany10CpuNodes640SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 640),
	)
}

func BenchmarkScheduleMany100CpuNodes3200SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 3200),
	)
}

func BenchmarkScheduleMany100CpuNodes6400SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 6400),
	)
}

func BenchmarkScheduleMany1000CpuNodes32000SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32000),
	)
}

func BenchmarkScheduleMany1000CpuNodes64000SmallJobs(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 64000),
	)
}

func BenchmarkScheduleMany100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(100, testfixtures.TestPriorities),
		),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 100),
	)
}

func BenchmarkScheduleMany1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(1000, testfixtures.TestPriorities),
		),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1000),
	)
}

func BenchmarkScheduleMany10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkScheduleMany(
		b,
		testfixtures.WithUsedResourcesNodes(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testfixtures.N32CpuNodes(10000, testfixtures.TestPriorities),
		),
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 10000),
	)
}

func BenchmarkScheduleManyResourceConstrained(b *testing.B) {
	nodes := append(append(
		testfixtures.N32CpuNodes(500, testfixtures.TestPriorities),
		testfixtures.N8GpuNodes(1, testfixtures.TestPriorities)...),
		testfixtures.N32CpuNodes(499, testfixtures.TestPriorities)...,
	)
	benchmarkScheduleMany(
		b,
		nodes,
		testfixtures.N1GpuJobs("A", testfixtures.PriorityClass0, 1),
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
