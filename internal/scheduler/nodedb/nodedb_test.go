package nodedb

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
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
	nodeDb, err := newNodeDbWithNodes([]*schedulerobjects.Node{})
	require.NoError(t, err)

	expected := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	assert.True(t, expected.Equal(nodeDb.TotalResources()))

	// Upserting nodes for the first time should increase the resource count.
	nodes := testfixtures.N32CpuNodes(2, testfixtures.TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	txn := nodeDb.Txn(true)
	for _, node := range nodes {
		err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
		require.NoError(t, err)
	}
	txn.Commit()

	assert.True(t, expected.Equal(nodeDb.TotalResources()))

	// Upserting new nodes should increase the resource count.
	nodes = testfixtures.N8GpuNodes(3, testfixtures.TestPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	txn = nodeDb.Txn(true)
	for _, node := range nodes {
		err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
		require.NoError(t, err)
	}
	txn.Commit()

	assert.True(t, expected.Equal(nodeDb.TotalResources()))
}

func TestSelectNodeForPod_NodeIdLabel_Success(t *testing.T) {
	nodes := testfixtures.N32CpuNodes(2, testfixtures.TestPriorities)
	nodeId := nodes[1].Id
	require.NotEmpty(t, nodeId)
	db, err := newNodeDbWithNodes(nodes)
	require.NoError(t, err)
	jobs := testfixtures.WithNodeSelectorJobs(
		map[string]string{schedulerconfig.NodeIdLabel: nodeId},
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
	)
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs, func(_ map[string]string) (string, int, int, bool, error) { return "", 1, 1, true, nil })
	for _, jctx := range jctxs {
		txn := db.Txn(false)
		node, err := db.SelectNodeForJobWithTxn(txn, jctx)
		txn.Abort()
		if !assert.NoError(t, err) {
			continue
		}
		pctx := jctx.PodSchedulingContext
		require.NotNil(t, node)
		assert.Equal(t, nodeId, node.Id)

		require.NotNil(t, pctx)
		assert.Equal(t, nodeId, pctx.NodeId)
		assert.Equal(t, 0, len(pctx.NumExcludedNodesByReason))
		assert.Empty(t, pctx.NumExcludedNodesByReason)
	}
}

func TestSelectNodeForPod_NodeIdLabel_Failure(t *testing.T) {
	nodes := testfixtures.N32CpuNodes(1, testfixtures.TestPriorities)
	nodeId := nodes[0].Id
	require.NotEmpty(t, nodeId)
	db, err := newNodeDbWithNodes(nodes)
	require.NoError(t, err)
	jobs := testfixtures.WithNodeSelectorJobs(
		map[string]string{schedulerconfig.NodeIdLabel: "this node does not exist"},
		testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 1),
	)
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs, func(_ map[string]string) (string, int, int, bool, error) { return "", 1, 1, true, nil })
	for _, jctx := range jctxs {
		txn := db.Txn(false)
		node, err := db.SelectNodeForJobWithTxn(txn, jctx)
		txn.Abort()
		if !assert.NoError(t, err) {
			continue
		}
		assert.Nil(t, node)

		pctx := jctx.PodSchedulingContext
		require.NotNil(t, pctx)
		assert.Equal(t, "", pctx.NodeId)
		assert.Equal(t, 1, len(pctx.NumExcludedNodesByReason))
	}
}

func TestNodeBindingEvictionUnbinding(t *testing.T) {
	node := testfixtures.Test8GpuNode(testfixtures.TestPriorities)
	nodeDb, err := newNodeDbWithNodes([]*schedulerobjects.Node{node})
	require.NoError(t, err)
	entry, err := nodeDb.GetNode(node.Id)
	require.NoError(t, err)

	jobFilter := func(job interfaces.LegacySchedulerJob) bool { return true }
	job := testfixtures.Test1GpuJob("A", testfixtures.PriorityClass0)
	request := schedulerobjects.ResourceListFromV1ResourceList(job.GetResourceRequirements().Requests)
	jobId := job.GetId()

	boundNode, err := bindJobToNode(testfixtures.TestPriorityClasses, job, entry)
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

	evictedBoundNode, err := bindJobToNode(testfixtures.TestPriorityClasses, job, evictedNode)
	require.NoError(t, err)

	_, _, err = EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, entry)
	require.Error(t, err)

	_, err = UnbindJobFromNode(testfixtures.TestPriorityClasses, job, entry)
	require.NoError(t, err)

	_, err = bindJobToNode(testfixtures.TestPriorityClasses, job, boundNode)
	require.Error(t, err)

	_, _, err = EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, evictedNode)
	require.Error(t, err)

	assertNodeAccountingEqual(t, entry, unboundNode)
	assertNodeAccountingEqual(t, entry, evictedUnboundNode)
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
	assert.True(t, expectedAllocatable.Equal(boundNode.AllocatableByPriority[priority]))

	assert.Empty(t, unboundNode.AllocatedByJobId)
	assert.Empty(t, unboundNode.AllocatedByQueue)
	assert.Empty(t, unboundNode.EvictedJobRunIds)
}

func assertNodeAccountingEqual(t *testing.T, node1, node2 *Node) {
	allocatable1 := schedulerobjects.QuantityByTAndResourceType[int32](node1.AllocatableByPriority)
	allocatable2 := schedulerobjects.QuantityByTAndResourceType[int32](node2.AllocatableByPriority)
	assert.True(
		t,
		allocatable1.Equal(allocatable2),
		"expected %v, but got %v",
		node1.AllocatableByPriority,
		node2.AllocatableByPriority,
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			node1.AllocatedByJobId,
			node2.AllocatedByJobId,
		),
		"expected %v, but got %v",
		node1.AllocatedByJobId,
		node2.AllocatedByJobId,
	)
	assert.True(
		t,
		armadamaps.DeepEqual(
			node1.AllocatedByQueue,
			node2.AllocatedByQueue,
		),
		"expected %v, but got %v",
		node1.AllocatedByQueue,
		node2.AllocatedByQueue,
	)
	assert.True(
		t,
		maps.Equal(
			node1.EvictedJobRunIds,
			node2.EvictedJobRunIds,
		),
		"expected %v, but got %v",
		node1.EvictedJobRunIds,
		node2.EvictedJobRunIds,
	)
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
			nodeDb, err := newNodeDbWithNodes([]*schedulerobjects.Node{})
			require.NoError(t, err)
			txn := nodeDb.Txn(true)
			jobs := []*jobdb.Job{
				testfixtures.Test1Cpu4GiJob("queue-alice", testfixtures.PriorityClass0),
				testfixtures.Test1Cpu4GiJob("queue-alice", testfixtures.PriorityClass3),
			}
			node := testfixtures.Test32CpuNode(testfixtures.TestPriorities)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, node)
			txn.Commit()
			require.NoError(t, err)
			entry, err := nodeDb.GetNode(node.Id)
			require.NoError(t, err)

			existingJobs := make([]interfaces.LegacySchedulerJob, len(jobs))
			for i, job := range jobs {
				existingJobs[i] = job
			}
			actualEvictions, _, err := EvictJobsFromNode(testfixtures.TestPriorityClasses, tc.jobFilter, existingJobs, entry)
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
			nodeDb, err := newNodeDbWithNodes(tc.Nodes)
			require.NoError(t, err)

			jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, tc.Jobs, func(_ map[string]string) (string, int, int, bool, error) { return "", 1, 1, true, nil })

			for i, jctx := range jctxs {
				ok, err := nodeDb.ScheduleMany([]*schedulercontext.JobSchedulingContext{jctx})
				require.NoError(t, err)
				pctx := jctx.PodSchedulingContext

				if !tc.ExpectSuccess[i] {
					assert.False(t, ok)
					if pctx != nil {
						assert.Equal(t, "", pctx.NodeId)
					}
					continue
				}

				assert.True(t, ok)
				require.NotNil(t, pctx)

				nodeId := pctx.NodeId
				if !tc.ExpectSuccess[i] {
					assert.Equal(t, "", nodeId)
					continue
				}
				require.NotEqual(t, "", nodeId)

				job := jctx.Job
				node, err := nodeDb.GetNode(nodeId)
				require.NoError(t, err)
				require.NotNil(t, node)
				expected := schedulerobjects.ResourceListFromV1ResourceList(job.GetResourceRequirements().Requests)
				actual, ok := node.AllocatedByJobId[job.GetId()]
				require.True(t, ok)
				assert.True(t, actual.Equal(expected))
			}
		})
	}
}

func TestScheduleMany(t *testing.T) {
	gangSuccess := testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 32))
	gangFailure := testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33))

	tests := map[string]struct {
		// Nodes to schedule across.
		Nodes []*schedulerobjects.Node
		// Schedule one group of jobs at a time.
		// Each group is composed of a slice of pods.
		Jobs [][]*jobdb.Job
		// For each group, whether we expect scheduling to succeed.
		ExpectSuccess []bool
	}{
		// Attempts to schedule 32 jobs with a minimum gang cardinality of 1 job. All jobs get scheduled.
		"simple success": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          [][]*jobdb.Job{gangSuccess},
			ExpectSuccess: []bool{true},
		},
		// Attempts to schedule 33 jobs with a minimum gang cardinality of 32 jobs. One fails, but the overall result is a success.
		"simple success with min cardinality": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          [][]*jobdb.Job{testfixtures.WithGangAnnotationsAndMinCardinalityJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33), 32)},
			ExpectSuccess: []bool{true},
		},
		// Attempts to schedule 33 jobs with a minimum gang cardinality of 33. The overall result fails.
		"simple failure with min cardinality": {
			Nodes:         testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Jobs:          [][]*jobdb.Job{gangFailure},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			Jobs: [][]*jobdb.Job{
				gangSuccess,
				gangFailure,
				gangSuccess,
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
			nodeDb, err := newNodeDbWithNodes(tc.Nodes)
			require.NoError(t, err)
			for i, jobs := range tc.Jobs {
				minCardinalityStr, ok := jobs[0].GetAnnotations()[configuration.GangMinimumCardinalityAnnotation]
				if !ok {
					minCardinalityStr = "1"
				}
				minCardinality, err := strconv.Atoi(minCardinalityStr)
				if err != nil {
					minCardinality = 1
				}
				extractGangInfo := func(_ map[string]string) (string, int, int, bool, error) {
					id, _ := uuid.NewUUID()
					return id.String(), 1, minCardinality, true, nil
				}

				jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs, extractGangInfo)
				ok, err = nodeDb.ScheduleMany(jctxs)
				require.NoError(t, err)
				assert.Equal(t, tc.ExpectSuccess[i], ok)
				for _, jctx := range jctxs {
					pctx := jctx.PodSchedulingContext
					require.NotNil(t, pctx)
					if tc.ExpectSuccess[i] && !jctx.ShouldFail {
						assert.NotEqual(t, "", pctx.NodeId)
					}
				}
			}
		})
	}
}

func benchmarkUpsert(nodes []*schedulerobjects.Node, b *testing.B) {
	nodeDb, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
		testfixtures.TestWellKnownNodeTypes,
	)
	require.NoError(b, err)
	txn := nodeDb.Txn(true)
	entries := make([]*Node, len(nodes))
	for i, node := range nodes {
		err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
		require.NoError(b, err)
		entry, err := nodeDb.GetNode(node.Id)
		require.NoError(b, err)
		entries[i] = entry
	}
	txn.Commit()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := nodeDb.UpsertMany(entries)
		require.NoError(b, err)
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
		testfixtures.TestWellKnownNodeTypes,
	)
	require.NoError(b, err)
	txn := nodeDb.Txn(true)
	for _, node := range nodes {
		err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node)
		require.NoError(b, err)
	}
	txn.Commit()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs, func(_ map[string]string) (string, int, int, bool, error) { return "", 1, 1, true, nil })
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

func newNodeDbWithNodes(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	nodeDb, err := NewNodeDb(
		testfixtures.TestPriorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
		testfixtures.TestWellKnownNodeTypes,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	for _, node := range nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
			return nil, err
		}
	}
	txn.Commit()
	return nodeDb, nil
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
