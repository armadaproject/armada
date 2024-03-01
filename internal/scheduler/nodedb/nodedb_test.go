package nodedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNodeDbSchema(t *testing.T) {
	schema, _, _ := nodeDbSchema(testfixtures.TestPriorities, testfixtures.TestResourceNames)
	assert.NoError(t, schema.Validate())
}

func TestNodeUnsafeCopy(t *testing.T) {
	node := &Node{
		Id:       "id",
		Index:    1,
		Executor: "executor",
		Name:     "name",
		Taints: []v1.Taint{
			{
				Key:   "foo",
				Value: "bar",
			},
		},
		Labels: map[string]string{
			"key": "value",
		},
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("32Gi"),
			},
		},
		Keys: [][]byte{
			{
				0, 1, 255,
			},
		},
		NodeTypeId: 123,
		AllocatableByPriority: schedulerobjects.AllocatableByPriorityAndResourceType{
			1: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("0"),
					"memory": resource.MustParse("0Gi"),
				},
			},
			2: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
			3: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("16"),
					"memory": resource.MustParse("32Gi"),
				},
			},
		},
		AllocatedByQueue: map[string]schedulerobjects.ResourceList{
			"queue": {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
		},
		AllocatedByJobId: map[string]schedulerobjects.ResourceList{
			"jobId": {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
		},
		EvictedJobRunIds: map[string]bool{
			"jobId":        false,
			"evictedJobId": true,
		},
	}
	nodeCopy := node.UnsafeCopy()
	// TODO(albin): Add more tests here.
	assert.Equal(t, node.Id, nodeCopy.Id)
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
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
	for _, jctx := range jctxs {
		txn := db.Txn(false)
		node, err := db.SelectNodeForJobWithTxn(txn, jctx)
		txn.Abort()
		require.NoError(t, err)
		pctx := jctx.PodSchedulingContext
		if assert.NotNil(t, node) {
			assert.Equal(t, nodeId, node.Id)
		}
		if assert.NotNil(t, pctx) {
			assert.Equal(t, nodeId, pctx.NodeId)
			assert.Empty(t, pctx.NumExcludedNodesByReason, "got %v", pctx.NumExcludedNodesByReason)
		}
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
	jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
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

	boundNode, err := nodeDb.bindJobToNode(entry, job, job.PodRequirements().Priority)
	require.NoError(t, err)

	unboundNode, err := nodeDb.UnbindJobFromNode(testfixtures.TestPriorityClasses, job, boundNode)
	require.NoError(t, err)

	unboundMultipleNode, err := nodeDb.UnbindJobsFromNode(testfixtures.TestPriorityClasses, []interfaces.LegacySchedulerJob{job}, boundNode)
	require.NoError(t, err)

	evictedJobs, evictedNode, err := nodeDb.EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, boundNode)
	require.NoError(t, err)
	assert.Equal(t, []interfaces.LegacySchedulerJob{job}, evictedJobs)

	evictedUnboundNode, err := nodeDb.UnbindJobFromNode(testfixtures.TestPriorityClasses, job, evictedNode)
	require.NoError(t, err)

	evictedBoundNode, err := nodeDb.bindJobToNode(evictedNode, job, job.PodRequirements().Priority)
	require.NoError(t, err)

	_, _, err = nodeDb.EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, entry)
	require.Error(t, err)

	_, err = nodeDb.UnbindJobFromNode(testfixtures.TestPriorityClasses, job, entry)
	require.NoError(t, err)

	_, err = nodeDb.bindJobToNode(boundNode, job, job.PodRequirements().Priority)
	require.Error(t, err)

	_, _, err = nodeDb.EvictJobsFromNode(testfixtures.TestPriorityClasses, jobFilter, []interfaces.LegacySchedulerJob{job}, evictedNode)
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
			actualEvictions, _, err := nodeDb.EvictJobsFromNode(testfixtures.TestPriorityClasses, tc.jobFilter, existingJobs, entry)
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

			jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, tc.Jobs)

			for i, jctx := range jctxs {
				nodeDbTxn := nodeDb.Txn(true)
				gctx := schedulercontext.NewGangSchedulingContext([]*schedulercontext.JobSchedulingContext{jctx})
				ok, err := nodeDb.ScheduleManyWithTxn(nodeDbTxn, gctx)
				require.NoError(t, err)

				require.Equal(t, tc.ExpectSuccess[i], ok)

				pctx := jctx.PodSchedulingContext

				if !ok {
					nodeDbTxn.Abort()
					if pctx != nil {
						assert.Equal(t, "", pctx.NodeId)
					}
					continue
				}

				nodeDbTxn.Commit()

				require.NotNil(t, pctx)
				nodeId := pctx.NodeId
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
			Jobs:          [][]*jobdb.Job{testfixtures.WithGangAnnotationsAndMinCardinalityJobs(32, testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33))},
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
				nodeDbTxn := nodeDb.Txn(true)
				jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
				gctx := schedulercontext.NewGangSchedulingContext(jctxs)
				ok, err := nodeDb.ScheduleManyWithTxn(nodeDbTxn, gctx)
				require.NoError(t, err)
				require.Equal(t, tc.ExpectSuccess[i], ok)
				if ok {
					nodeDbTxn.Commit()
				} else {
					nodeDbTxn.Abort()
					// We make no assertions about pctx in this case; if some of
					// the jobs in the gang were scheduled successfully and
					// others were not, then pctx.NodeId will be inconsistent
					// until the gang is returned back to the gang scheduler.
					continue
				}
				for _, jctx := range jctxs {
					pctx := jctx.PodSchedulingContext
					require.NotNil(t, pctx)
					if !jctx.ShouldFail {
						assert.NotEqual(t, "", pctx.NodeId)
					}
				}
			}
		})
	}
}

func TestAwayNodeTypes(t *testing.T) {
	priorityClasses := map[string]types.PriorityClass{
		"armada-preemptible-away": {
			Priority:    30000,
			Preemptible: true,

			AwayNodeTypes: []types.AwayNodeType{
				{Priority: 29000, WellKnownNodeTypeName: "whale"},
			},
		},
	}

	nodeDb, err := NewNodeDb(
		priorityClasses,
		testfixtures.TestMaxExtraNodesToConsider,
		testfixtures.TestResources,
		testfixtures.TestIndexedTaints,
		testfixtures.TestIndexedNodeLabels,
		[]configuration.WellKnownNodeType{
			{
				Name: "whale",
				Taints: []v1.Taint{
					{
						Key:    "whale",
						Value:  "true",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
	)
	require.NoError(t, err)

	nodeDbTxn := nodeDb.Txn(true)
	node := testfixtures.Test32CpuNode([]int32{29000, 30000})
	node.Taints = append(
		node.Taints,
		v1.Taint{
			Key:    "whale",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	)
	require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, nil, node))

	jobId := util.ULID()
	job := testfixtures.TestJob(
		testfixtures.TestQueue,
		jobId,
		"armada-preemptible-away",
		testfixtures.Test1Cpu4GiPodReqs(testfixtures.TestQueue, jobId, 30000),
	)
	jctx := schedulercontext.JobSchedulingContextFromJob(priorityClasses, job)
	require.Empty(t, jctx.AdditionalTolerations)
	gctx := schedulercontext.NewGangSchedulingContext([]*schedulercontext.JobSchedulingContext{jctx})

	ok, err := nodeDb.ScheduleManyWithTxn(nodeDbTxn, gctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(
		t,
		[]v1.Toleration{
			{
				Key:    "whale",
				Value:  "true",
				Effect: v1.TaintEffectNoSchedule,
			},
		},
		jctx.AdditionalTolerations,
	)
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
		jctxs := schedulercontext.JobSchedulingContextsFromJobs(testfixtures.TestPriorityClasses, jobs)
		gctx := schedulercontext.NewGangSchedulingContext(jctxs)
		txn := nodeDb.Txn(true)
		_, err := nodeDb.ScheduleManyWithTxn(txn, gctx)
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
	reason := &UntoleratedTaint{
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
