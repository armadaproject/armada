package optimiser

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

var (
	queueA = testfixtures.SingleQueueWithPriorityFactor("A", 10)
	queueB = testfixtures.SingleQueueWithPriorityFactor("B", 10)
	queueC = testfixtures.SingleQueueWithPriorityFactor("C", 10)
	queueD = testfixtures.SingleQueueWithPriorityFactor("D", 5)
)

func TestSchedule_NodeChecks(t *testing.T) {
	job := testfixtures.Test1Cpu16GiJob("A", testfixtures.PriorityClass1)
	jobWithNodeSelector := testfixtures.WithNodeSelectorJob(map[string]string{"armadaproject.io/nodeType": "special"}, job)
	tests := map[string]struct {
		node          *internaltypes.Node
		job           *jobdb.Job
		expectSuccess bool
	}{
		"job matches node": {
			node:          testfixtures.Test32CpuNode(testfixtures.TestPriorities),
			job:           job,
			expectSuccess: true,
		},
		"node has untolerated taints": {
			node:          testfixtures.TestTainted32CpuNode(testfixtures.TestPriorities),
			job:           job,
			expectSuccess: false,
		},
		"node doesn't match selector": {
			node:          testfixtures.TestTainted32CpuNode(testfixtures.TestPriorities),
			job:           jobWithNodeSelector,
			expectSuccess: false,
		},
		"node too small": {
			node: testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
				"cpu":    resourceFromString("1"),
				"memory": resourceFromString("5Gi"),
			}),
			job:           job,
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			job := testfixtures.Test1Cpu16GiJob("A", testfixtures.PriorityClass1)
			jctx := context.JobSchedulingContextFromJob(job)
			jobDb := testfixtures.JobDb
			sctx := setUpSctx(t, []*api.Queue{queueA}, []*jobdb.Job{}, tc.node.GetAllocatableResources())
			schedContext := &SchedulingContext{Sctx: sctx, Queues: map[string]*QueueContext{}}
			nodeScheduler := NewPreemptingNodeScheduler(jobDb.ReadTxn(), nil)

			result, err := nodeScheduler.Schedule(schedContext, jctx, tc.node)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectSuccess, result.scheduled)
			assert.Equal(t, float64(0), result.schedulingCost)
			assert.Empty(t, result.queueCostChanges)
			assert.Empty(t, result.jobIdsToPreempt)
			assert.Equal(t, float64(0), result.maximumQueueImpact)
			assert.Equal(t, result.node, tc.node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func TestSchedule_JobChecks(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu":    resourceFromString("10"),
		"memory": resourceFromString("25Gi"),
	})
	jobToSchedule := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	})
	existingJob := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	})

	tests := map[string]struct {
		existingJob                    *jobdb.Job
		existingJobScheduledAtPriority int32
		maximumJobSizeToPreempt        *armadaresource.ComputeResources
		expectSuccess                  bool
		expectError                    bool
	}{
		"preempts job - preempted job scheduled in current round": {
			existingJob:                    existingJob.DeepCopy().WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  true,
		},
		"preempts job - preempted job scheduled in previous round": {
			existingJob: existingJob.DeepCopy().WithQueued(false).WithNewRun(
				node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool,
				testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority),
			expectSuccess: true,
		},
		"will not preempt non-preemptible jobs": {
			existingJob:                    existingJob.DeepCopy().WithQueued(true).WithPriorityClass(testfixtures.TestPriorityClasses[testfixtures.PriorityClass2NonPreemptible]),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  false,
		},
		"will not preempt gang jobs": {
			existingJob:                    testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{existingJob.DeepCopy().WithQueued(true).DeepCopy(), existingJob.WithQueued(true).DeepCopy()})[0],
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  false,
		},
		"will not preempt jobs larger than maximumJobSizeToPreempt": {
			existingJob:                    existingJob.DeepCopy().WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			maximumJobSizeToPreempt: &armadaresource.ComputeResources{
				"cpu": resource.MustParse("1"),
			},
			expectSuccess: false,
		},
		"will not preempt jobs scheduled at higher priority - preempted job scheduled in current round": {
			existingJob:                    existingJob.DeepCopy().WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority,
			expectSuccess:                  false,
		},
		"will not preempt jobs scheduled at higher priority - preempted job scheduled in previous round": {
			existingJob: existingJob.DeepCopy().WithQueued(false).WithNewRun(
				node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool,
				testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority),
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := node.DeepCopyNilKeys()
			jctx := context.JobSchedulingContextFromJob(jobToSchedule)
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			nodeDbTxn := nodeDb.Txn(true)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, []*jobdb.Job{tc.existingJob}, node.DeepCopyNilKeys())
			require.NoError(t, err)
			nodeDbTxn.Commit()
			node, err = nodeDb.GetNode(node.GetId())
			require.NoError(t, err)
			jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{tc.existingJob})

			totalResource := testfixtures.CpuMem("100", "2000Gi")
			sctx := setUpSctx(t, []*api.Queue{queueA, queueB}, []*jobdb.Job{tc.existingJob}, totalResource)
			if tc.existingJob.Queued() {
				existingJctx := context.JobSchedulingContextFromJob(tc.existingJob)
				existingJctx.PodSchedulingContext = &context.PodSchedulingContext{
					ScheduledAtPriority: tc.existingJobScheduledAtPriority,
					NodeId:              node.GetId(),
				}
				_, err = sctx.AddJobSchedulingContext(existingJctx)
				require.NoError(t, err)
			}

			var maximumJobSizeToPreempt *internaltypes.ResourceList
			if tc.maximumJobSizeToPreempt != nil {
				maxJobSize := testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(*tc.maximumJobSizeToPreempt)
				maximumJobSizeToPreempt = &maxJobSize
			}
			nodeScheduler := NewPreemptingNodeScheduler(jobDb.ReadTxn(), maximumJobSizeToPreempt)

			result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectSuccess, result.scheduled)
			if tc.expectSuccess {
				assert.Equal(t, float64(0.08), result.schedulingCost)
				assert.Equal(t, map[string]float64{"B": -0.08}, result.queueCostChanges)
				assert.Equal(t, []string{tc.existingJob.Id()}, result.jobIdsToPreempt)
				assert.Equal(t, float64(1), result.maximumQueueImpact)
			} else {
				assert.Equal(t, float64(0), result.schedulingCost)
				assert.Empty(t, result.queueCostChanges)
				assert.Empty(t, result.jobIdsToPreempt)
				assert.Equal(t, float64(0), result.maximumQueueImpact)
			}
			assert.Equal(t, result.node, node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func setUpSctx(t *testing.T, queues []*api.Queue, existingJobs []*jobdb.Job, totalResource internaltypes.ResourceList) *context.SchedulingContext {
	jobsByQueue := armadaslices.GroupByFunc(existingJobs, func(job *jobdb.Job) string {
		return job.Queue()
	})
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		totalResource,
		testfixtures.TestSchedulingConfig(),
	)
	require.NoError(t, err)

	sctx := context.NewSchedulingContext(
		testfixtures.TestPool,
		fairnessCostProvider,
		nil,
		totalResource,
	)

	for _, q := range queues {
		existingAllocation := map[string]internaltypes.ResourceList{}
		for _, job := range jobsByQueue[q.Name] {
			if !job.Queued() {
				if _, exists := existingAllocation[job.PriorityClassName()]; !exists {
					existingAllocation[job.PriorityClassName()] = internaltypes.ResourceList{}
				}
				existingAllocation[job.PriorityClassName()] = existingAllocation[job.PriorityClassName()].Add(job.AllResourceRequirements())
			}
		}

		weight := 1.0 / float64(q.PriorityFactor)
		unlimitedDemand := testfixtures.CpuMem("10000", "100000Gi")
		err := sctx.AddQueueSchedulingContext(
			q.Name, weight, weight,
			existingAllocation,
			unlimitedDemand,
			unlimitedDemand,
			nil,
		)
		require.NoError(t, err)
	}
	sctx.UpdateFairShares()
	return sctx
}

func markedScheduledOnNode(jobs []*jobdb.Job, node *internaltypes.Node) []*jobdb.Job {
	result := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job.WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[job.PriorityClassName()].Priority))
	}
	return result
}

func TestSchedule_PreemptsExpectedJobs(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{"cpu": resourceFromString("10")})
	bigNode := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{"cpu": resourceFromString("18")})
	jobToSchedule := createTestCpuJob("A", 8)
	smallJobToSchedule := createTestCpuJob("A", 3)
	bigJobToSchedule := createTestCpuJob("A", 12)
	tests := map[string]struct {
		jobToSchedule              *jobdb.Job
		queues                     []*api.Queue
		node                       *internaltypes.Node
		extraDemand                *armadaresource.ComputeResources
		extraTotalResource         *armadaresource.ComputeResources
		jobsOnNode                 []*jobdb.Job
		orderedPreemptedJobIndexes []int
		expectedResult             *nodeSchedulingResult
	}{
		"preempt jobs - multiple same queue": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJob("B", 4), createTestCpuJob("B", 4)},
			orderedPreemptedJobIndexes: []int{1, 0}, // B2, B1 - Will preempt index 1 first, as it is the youngest
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.8,
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.8},
			},
		},
		"preempt jobs - multiple different queue": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJob("B", 2), createTestCpuJob("B", 2), createTestCpuJob("C", 2), createTestCpuJob("C", 2)},
			orderedPreemptedJobIndexes: []int{3, 1, 2}, // C2, B1, C1
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.2, "C": -0.4},
			},
		},
		"preempt jobs - mixed queue priorities": {
			jobToSchedule: bigJobToSchedule,
			queues:        []*api.Queue{queueA, queueB, queueD},
			node:          bigNode,
			// This is so we can have all queues below fairshare, total resource is 100
			extraTotalResource:         &armadaresource.ComputeResources{"cpu": resource.MustParse("82")},
			jobsOnNode:                 armadaslices.Concatenate(createNTestCpuJob("B", 2, 3), createNTestCpuJob("D", 2, 6)),
			orderedPreemptedJobIndexes: []int{8, 7, 2, 6, 5, 1}, // D6, D5, B3, D4, D3, B2
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.12,
				maximumQueueImpact: float64(2) / 3,
				queueCostChanges:   map[string]float64{"B": -0.04, "D": -0.08},
			},
		},
		"preempt jobs - smallest first": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJob("B", 2), createTestCpuJob("B", 4)},
			orderedPreemptedJobIndexes: []int{0, 1}, // B1, B2
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.6},
			},
		},
		"preempting jobs above fairshare - 0 cost": {
			jobToSchedule:              smallJobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			extraDemand:                &armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
			jobsOnNode:                 []*jobdb.Job{createTestCpuJob("B", 2), createTestCpuJob("B", 2), createTestCpuJob("B", 4)},
			orderedPreemptedJobIndexes: []int{1}, // B2
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.0,                           // Only jobs above fairshare preempted
				maximumQueueImpact: 0.25,                          // Queue B has 8 cores scheduled, 2 cores get preempted
				queueCostChanges:   map[string]float64{"B": -0.2}, // Preempted was 2 cores, 10 cores in total nodeDb, so 0.2 cost
			},
		},
		"preempting jobs of lower priority - 0 cost": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPriorityClass("B", 2, testfixtures.PriorityClass0), createTestCpuJobWithPriorityClass("B", 2, testfixtures.PriorityClass0)},
			orderedPreemptedJobIndexes: []int{1}, // B2
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.0, // Only jobs scheduled at a lower priority preempted
				maximumQueueImpact: 0.5,
				queueCostChanges:   map[string]float64{"B": -0.2},
			},
		},

		"preempt jobs - expected order": {
			jobToSchedule: jobToSchedule,
			queues:        []*api.Queue{queueA, queueB, queueC},
			node:          node,
			jobsOnNode: []*jobdb.Job{
				createTestCpuJobWithPriorityClass("B", 2, testfixtures.PriorityClass0), createTestCpuJob("B", 1), createTestCpuJob("B", 2),
				createTestCpuJob("C", 2), createTestCpuJob("C", 2), createTestCpuJob("C", 1),
			},
			orderedPreemptedJobIndexes: []int{0, 5, 1, 4, 3}, // B1 (low prio), C3 (small), B2 (small), C2, C1
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.5,
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.3, "C": -0.5},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := tc.node.DeepCopyNilKeys()
			jctx := context.JobSchedulingContextFromJob(tc.jobToSchedule)
			jobsOnNode := markedScheduledOnNode(tc.jobsOnNode, node)

			extraCapacity := testfixtures.TestResourceListFactory.MakeAllZero()
			if tc.extraTotalResource != nil {
				extraCapacity = testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(*tc.extraTotalResource)
			}
			sctx := setUpSctx(t, tc.queues, jobsOnNode, node.GetAllocatableResources().Add(extraCapacity))

			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			nodeDbTxn := nodeDb.Txn(true)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, jobsOnNode, node.DeepCopyNilKeys())
			require.NoError(t, err)
			nodeDbTxn.Commit()
			node, err = nodeDb.GetNode(node.GetId())
			require.NoError(t, err)
			jobDb := testfixtures.NewJobDbWithJobs(jobsOnNode)
			nodeScheduler := NewPreemptingNodeScheduler(jobDb.ReadTxn(), nil)

			result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
			assert.NoError(t, err)

			for queue, costChange := range result.queueCostChanges {
				result.queueCostChanges[queue] = roundFloatHighPrecision(costChange)
			}
			expectedPreemptedJobIds := make([]string, 0, len(tc.orderedPreemptedJobIndexes))
			for _, index := range tc.orderedPreemptedJobIndexes {
				expectedPreemptedJobIds = append(expectedPreemptedJobIds, tc.jobsOnNode[index].Id())
			}
			tc.expectedResult.jobIdsToPreempt = expectedPreemptedJobIds
			assert.Equal(t, tc.expectedResult.scheduled, result.scheduled)
			assert.Equal(t, tc.expectedResult.schedulingCost, roundFloatHighPrecision(result.schedulingCost))
			assert.Equal(t, tc.expectedResult.jobIdsToPreempt, result.jobIdsToPreempt)
			assert.Equal(t, tc.expectedResult.queueCostChanges, result.queueCostChanges)
			assert.Equal(t, roundFloatHighPrecision(tc.expectedResult.maximumQueueImpact), roundFloatHighPrecision(result.maximumQueueImpact))
			assert.Equal(t, result.node, node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func TestSchedule_Errors_WhenInformationMissingFromState(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu":    resourceFromString("10"),
		"memory": resourceFromString("25Gi"),
	})
	jobToSchedule := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	})
	existingJob := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	}).WithQueued(true)

	sctx := setUpSctx(t, []*api.Queue{queueA}, []*jobdb.Job{}, node.GetAllocatableResources())
	jctx := context.JobSchedulingContextFromJob(jobToSchedule)
	nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
	require.NoError(t, err)
	nodeDbTxn := nodeDb.Txn(true)
	err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, []*jobdb.Job{existingJob}, node.DeepCopyNilKeys())
	require.NoError(t, err)
	nodeDbTxn.Commit()
	node, err = nodeDb.GetNode(node.GetId())
	require.NoError(t, err)
	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{})

	schedContext := &SchedulingContext{Sctx: sctx, Queues: map[string]*QueueContext{}}
	for queue, qctx := range sctx.QueueSchedulingContexts {
		schedContext.Queues[queue] = &QueueContext{
			Name:      queue,
			Fairshare: qctx.DemandCappedAdjustedFairShare,
		}
	}

	nodeScheduler := NewPreemptingNodeScheduler(jobDb.ReadTxn(), nil)
	result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in jobDb")
	assert.Nil(t, result)

	// Add existing job to jobdb
	txn := jobDb.WriteTxn()
	err = txn.Upsert([]*jobdb.Job{existingJob})
	assert.NoError(t, err)
	txn.Commit()
	nodeScheduler = NewPreemptingNodeScheduler(jobDb.ReadTxn(), nil)

	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find queue context")
	assert.Nil(t, result)

	// Create sctx with QueueB now included
	sctx = setUpSctx(t, []*api.Queue{queueA, queueB}, []*jobdb.Job{existingJob}, node.GetAllocatableResources())
	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find job context")
	assert.Nil(t, result)

	// Add existing jctx to sctx
	existingJctx := context.JobSchedulingContextFromJob(existingJob)
	_, err = sctx.AddJobSchedulingContext(existingJctx)
	assert.NoError(t, err)

	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no pod scheduling context exists on jctx")
	assert.Nil(t, result)

	// Add pctx to existing job jctx
	existingJctx.PodSchedulingContext = &context.PodSchedulingContext{
		ScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
		NodeId:              node.GetId(),
	}
	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.NoError(t, err)
	assert.True(t, result.scheduled)
}

func TestIsTooLargeToEvict(t *testing.T) {
	job := testfixtures.Test1Cpu4GiJob("test", testfixtures.PriorityClass1)

	limitLowerThanJob1 := testfixtures.CpuMem("1", "3Gi")
	limitLowerThanJob2 := testfixtures.CpuMem("0.5", "4Gi")
	limitLowerThanJob3 := testfixtures.CpuMem("0.5", "3Gi")
	limitEqualToJob := testfixtures.CpuMem("1", "4Gi")
	limitAboveJob1 := testfixtures.CpuMem("1", "8Gi")
	limitAboveJob2 := testfixtures.CpuMem("2", "4Gi")
	limitAboveJob3 := testfixtures.CpuMem("2", "8Gi")

	assert.False(t, isTooLargeToEvict(job, nil)) // Allow any job if no limit set
	assert.False(t, isTooLargeToEvict(job, &limitAboveJob1))
	assert.False(t, isTooLargeToEvict(job, &limitAboveJob2))
	assert.False(t, isTooLargeToEvict(job, &limitAboveJob3))
	assert.False(t, isTooLargeToEvict(job, &limitEqualToJob))
	assert.True(t, isTooLargeToEvict(job, &limitLowerThanJob1))
	assert.True(t, isTooLargeToEvict(job, &limitLowerThanJob2))
	assert.True(t, isTooLargeToEvict(job, &limitLowerThanJob3))
}

func NewNodeDb(config configuration.SchedulingConfig) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		config.PriorityClasses,
		config.IndexedResources,
		config.IndexedTaints,
		config.IndexedNodeLabels,
		config.WellKnownNodeTypes,
		testfixtures.TestResourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

func createTestCpuJob(queueName string, cpu int) *jobdb.Job {
	return createTestCpuJobWithPriorityClass(queueName, cpu, testfixtures.PriorityClass2)
}

func createTestCpuJobWithPriorityClass(queueName string, cpu int, priorityClassName string) *jobdb.Job {
	return testfixtures.TestJobWithResources(queueName, priorityClassName, v1.ResourceList{
		"cpu": resource.MustParse(strconv.Itoa(cpu)),
	})
}

func createNTestCpuJob(queueName string, cpu int, count int) []*jobdb.Job {
	result := make([]*jobdb.Job, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, createTestCpuJob(queueName, cpu))
	}
	return result
}

func resourceFromString(s string) *resource.Quantity {
	qty := resource.MustParse(s)
	return &qty
}
