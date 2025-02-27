package optimiser

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
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
			node: testfixtures.TestNode(testfixtures.TestPriorities, map[string]resource.Quantity{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("5Gi"),
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

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				tc.node.GetAllocatableResources(),
				testfixtures.TestSchedulingConfig(),
			)
			require.NoError(t, err)
			sctx := context.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				nil,
				tc.node.GetAllocatableResources(),
			)

			weight := float64(1 / 100)
			err = sctx.AddQueueSchedulingContext(
				"A",
				weight,
				weight,
				map[string]internaltypes.ResourceList{"A": job.AllResourceRequirements()},
				job.AllResourceRequirements(),
				job.AllResourceRequirements(),
				nil,
			)
			sctx.UpdateFairShares()

			require.NoError(t, err)
			schedContext := &SchedulingContext{Sctx: sctx, Queues: map[string]*QueueContext{}}

			nodeScheduler := NewNodeScheduler(jobDb.ReadTxn(), nil)

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
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]resource.Quantity{
		"cpu":    resource.MustParse("10"),
		"memory": resource.MustParse("25Gi"),
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
			existingJob:                    existingJob.WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  true,
		},
		"preempts job - preempted job scheduled in previous round": {
			existingJob: existingJob.WithQueued(false).WithNewRun(
				node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool,
				testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority),
			expectSuccess: true,
		},
		"will not preempt non-preemptible jobs": {
			existingJob:                    existingJob.WithQueued(true).WithPriorityClass(testfixtures.TestPriorityClasses[testfixtures.PriorityClass2NonPreemptible]),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  false,
		},
		"will not preempt gang jobs": {
			existingJob:                    testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{existingJob.WithQueued(true).DeepCopy(), existingJob.WithQueued(true).DeepCopy()})[0],
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			expectSuccess:                  false,
		},
		"will not preempt jobs larger than maximumJobSizeToPreempt": {
			existingJob:                    existingJob.WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority,
			maximumJobSizeToPreempt: &armadaresource.ComputeResources{
				"cpu": resource.MustParse("1"),
			},
			expectSuccess: false,
		},
		"will not preempt jobs scheduled at higher priority - preempted job scheduled in current round": {
			existingJob:                    existingJob.WithQueued(true),
			existingJobScheduledAtPriority: testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority,
			expectSuccess:                  false,
		},
		"will not preempt jobs scheduled at higher priority - preempted job scheduled in previous round": {
			existingJob: existingJob.WithQueued(false).WithNewRun(
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

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				node.GetAllocatableResources(),
				testfixtures.TestSchedulingConfig(),
			)
			require.NoError(t, err)
			sctx := context.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				nil,
				node.GetAllocatableResources(),
			)

			var weight float64 = 1
			weight = 1 / float64(2)
			err = sctx.AddQueueSchedulingContext(
				"A",
				weight,
				weight,
				map[string]internaltypes.ResourceList{"A": jobToSchedule.AllResourceRequirements()},
				jobToSchedule.AllResourceRequirements(),
				jobToSchedule.AllResourceRequirements(),
				nil,
			)
			require.NoError(t, err)
			err = sctx.AddQueueSchedulingContext(
				"B",
				weight,
				weight,
				map[string]internaltypes.ResourceList{},
				tc.existingJob.AllResourceRequirements(),
				tc.existingJob.AllResourceRequirements(),
				nil,
			)
			require.NoError(t, err)
			sctx.UpdateFairShares()
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
			nodeScheduler := NewNodeScheduler(jobDb.ReadTxn(), maximumJobSizeToPreempt)

			result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectSuccess, result.scheduled)
			if tc.expectSuccess {
				assert.Equal(t, float64(0.8), result.schedulingCost)
				assert.Equal(t, map[string]float64{"B": -0.8}, result.queueCostChanges)
				assert.Equal(t, []string{tc.existingJob.Id()}, result.jobIdsToPreempt)
				assert.Equal(t, float64(1.6), result.maximumQueueImpact)
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

func TestSchedule_PreemptsExpectedJobs(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]resource.Quantity{
		"cpu": resource.MustParse("10"),
	})
	jobToSchedule := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("8"),
	})
	jobToSchedule2 := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("3"),
	})

	jobB1 := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)
	jobB2 := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)

	jobB1LowPrio := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass0, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass0].Priority)
	jobB2LowPrio := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass0, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass0].Priority)

	jobB1Large := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("4"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)
	jobB2Large := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("4"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)

	jobC1 := testfixtures.TestJobWithResources("C", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)
	jobC2 := testfixtures.TestJobWithResources("C", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu": resource.MustParse("2"),
	}).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[testfixtures.PriorityClass2].Priority)

	tests := map[string]struct {
		jobToSchedule     *jobdb.Job
		extraDemand       *armadaresource.ComputeResources
		jobsOnNodeByQueue map[string][]*jobdb.Job
		expectedResult    *nodeSchedulingResult
	}{
		"preempt jobs - multiple same queue": {
			jobToSchedule:     jobToSchedule,
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1Large, jobB2Large}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.8,
				jobIdsToPreempt:    []string{jobB2Large.Id(), jobB1Large.Id()}, // Will preempt job2 first, as it is the youngest
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.8},
			},
		},
		"preempt jobs - multiple different queue": {
			jobToSchedule:     jobToSchedule,
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1, jobB2}, "C": {jobC1, jobC2}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				jobIdsToPreempt:    []string{jobC2.Id(), jobB2.Id(), jobC1.Id()},
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.2, "C": -0.4},
			},
		},
		// TODO
		"preempt jobs - mixed queue priorities": {
			jobToSchedule:     jobToSchedule,
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1, jobB2}, "C": {jobC1, jobC2}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				jobIdsToPreempt:    []string{jobC2.Id(), jobB2.Id(), jobC1.Id()},
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.2, "C": -0.4},
			},
		},
		"preempt jobs - smallest first": {
			jobToSchedule:     jobToSchedule,
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1, jobB1Large}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				jobIdsToPreempt:    []string{jobB1.Id(), jobB1Large.Id()},
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.6},
			},
		},

		"preempting jobs above fairshare - 0 cost": {
			jobToSchedule:     jobToSchedule2,
			extraDemand:       &armadaresource.ComputeResources{"cpu": resource.MustParse("10")},
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1, jobB2, jobB1Large}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.0, // Only jobs above fairshare preempted
				jobIdsToPreempt:    []string{jobB2.Id()},
				maximumQueueImpact: 0.25,                          // Queue B has 8 cores scheduled, 2 cores get preempted
				queueCostChanges:   map[string]float64{"B": -0.2}, // Preempted was 2 cores, 10 cores in total nodeDb, so 0.2 cost
			},
		},

		"preempting jobs of lower priority - 0 cost": {
			jobToSchedule:     jobToSchedule,
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1LowPrio, jobB2LowPrio}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.0, // Only jobs scheduled at a lower priority preempted
				jobIdsToPreempt:    []string{jobB2LowPrio.Id()},
				maximumQueueImpact: 0.5,
				queueCostChanges:   map[string]float64{"B": -0.2},
			},
		},

		"preempt jobs - expected order": {
			jobsOnNodeByQueue: map[string][]*jobdb.Job{"B": {jobB1, jobB2}, "C": {jobC1, jobC2}},
			expectedResult: &nodeSchedulingResult{
				scheduled:          true,
				schedulingCost:     0.6,
				jobIdsToPreempt:    []string{jobC2.Id(), jobB2.Id(), jobC1.Id()}, // Will preempt second job first, as it is the youngest
				maximumQueueImpact: 1,
				queueCostChanges:   map[string]float64{"B": -0.2, "C": -0.4},
			},
		},

		//Preempt multiple - same queue
		//Preempt multiple - different queue (interleaved)
		//Preempt in expected order:
		// - Smallest first
		// - Interleaved by queue
		//Preempt above fairshare, cost=0
		//Preempt lower priority class, cost=0
		//Preempt in expected order (interleaved)
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := node.DeepCopyNilKeys()
			jctx := context.JobSchedulingContextFromJob(tc.jobToSchedule)

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				node.GetAllocatableResources(),
				testfixtures.TestSchedulingConfig(),
			)
			require.NoError(t, err)
			sctx := context.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				nil,
				node.GetAllocatableResources(),
			)

			var weight float64 = 1
			weight = 1 / float64(2)
			queueDemand := tc.jobToSchedule.AllResourceRequirements()
			if tc.extraDemand != nil {
				queueDemand = queueDemand.Add(testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(*tc.extraDemand))
			}
			err = sctx.AddQueueSchedulingContext(
				"A",
				weight,
				weight,
				map[string]internaltypes.ResourceList{},
				queueDemand,
				queueDemand,
				nil,
			)
			require.NoError(t, err)

			allJobs := []*jobdb.Job{}
			for queueName, jobs := range tc.jobsOnNodeByQueue {
				allJobs = append(allJobs, jobs...)
				totalDemand := testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{})

				for _, job := range jobs {
					totalDemand = totalDemand.Add(job.AllResourceRequirements())
				}

				err = sctx.AddQueueSchedulingContext(
					queueName,
					weight,
					weight,
					map[string]internaltypes.ResourceList{testfixtures.PriorityClass2: totalDemand},
					totalDemand,
					totalDemand,
					nil,
				)
				require.NoError(t, err)
			}

			sctx.UpdateFairShares()

			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			nodeDbTxn := nodeDb.Txn(true)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, allJobs, node.DeepCopyNilKeys())
			require.NoError(t, err)
			nodeDbTxn.Commit()
			node, err = nodeDb.GetNode(node.GetId())
			require.NoError(t, err)
			jobDb := testfixtures.NewJobDbWithJobs(allJobs)

			nodeScheduler := NewNodeScheduler(jobDb.ReadTxn(), nil)
			result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedResult.scheduled, result.scheduled)
			assert.Equal(t, tc.expectedResult.schedulingCost, toFixed(result.schedulingCost, 3))
			assert.Equal(t, tc.expectedResult.jobIdsToPreempt, result.jobIdsToPreempt)
			assert.Equal(t, tc.expectedResult.queueCostChanges, result.queueCostChanges)
			assert.Equal(t, tc.expectedResult.maximumQueueImpact, result.maximumQueueImpact)
			assert.Equal(t, result.node, node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

//Preempt multiple
//Preempt above fairshare, cost=0
//Preempt lower priority class, cost=0
//Preempt in expected order (interleaved)

func TestSchedule_Errors_WhenInformationMissingFromState(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]resource.Quantity{
		"cpu":    resource.MustParse("10"),
		"memory": resource.MustParse("25Gi"),
	})
	jobToSchedule := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	})
	existingJob := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	}).WithQueued(true)

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

	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		node.GetAllocatableResources(),
		testfixtures.TestSchedulingConfig(),
	)
	require.NoError(t, err)
	sctx := context.NewSchedulingContext(
		testfixtures.TestPool,
		fairnessCostProvider,
		nil,
		node.GetAllocatableResources(),
	)

	var weight float64 = 1
	weight = 1 / float64(2)
	err = sctx.AddQueueSchedulingContext(
		"A",
		weight,
		weight,
		map[string]internaltypes.ResourceList{"A": jobToSchedule.AllResourceRequirements()},
		jobToSchedule.AllResourceRequirements(),
		jobToSchedule.AllResourceRequirements(),
		nil,
	)
	require.NoError(t, err)

	schedContext := &SchedulingContext{Sctx: sctx, Queues: map[string]*QueueContext{}}
	for queue, qctx := range sctx.QueueSchedulingContexts {
		schedContext.Queues[queue] = &QueueContext{
			Name:      queue,
			Fairshare: qctx.AdjustedFairShare,
		}
	}

	nodeScheduler := NewNodeScheduler(jobDb.ReadTxn(), nil)
	result, err := nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in jobDb")
	assert.Nil(t, result)

	txn := jobDb.WriteTxn()
	err = txn.Upsert([]*jobdb.Job{existingJob})
	assert.NoError(t, err)
	txn.Commit()
	nodeScheduler = NewNodeScheduler(jobDb.ReadTxn(), nil)

	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find queue context")
	assert.Nil(t, result)

	err = sctx.AddQueueSchedulingContext(
		"B",
		weight,
		weight,
		map[string]internaltypes.ResourceList{},
		existingJob.AllResourceRequirements(),
		existingJob.AllResourceRequirements(),
		nil,
	)
	require.NoError(t, err)
	sctx.UpdateFairShares()

	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find job context")
	assert.Nil(t, result)

	existingJctx := context.JobSchedulingContextFromJob(existingJob)
	_, err = sctx.AddJobSchedulingContext(existingJctx)
	assert.NoError(t, err)

	result, err = nodeScheduler.Schedule(FromSchedulingContext(sctx), jctx, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no pod scheduling context exists on jctx")
	assert.Nil(t, result)

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
