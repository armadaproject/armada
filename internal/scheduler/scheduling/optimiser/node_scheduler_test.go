package optimiser

import (
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

			schedContext := &SchedulingContext{Sctx: sctx, Queues: map[string]*QueueContext{}}
			for queue, qctx := range sctx.QueueSchedulingContexts {
				schedContext.Queues[queue] = &QueueContext{
					Name:      queue,
					Fairshare: qctx.AdjustedFairShare,
				}
			}

			var maximumJobSizeToPreempt *internaltypes.ResourceList
			if tc.maximumJobSizeToPreempt != nil {
				maxJobSize := testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(*tc.maximumJobSizeToPreempt)
				maximumJobSizeToPreempt = &maxJobSize
			}
			nodeScheduler := NewNodeScheduler(jobDb.ReadTxn(), maximumJobSizeToPreempt)

			result, err := nodeScheduler.Schedule(schedContext, jctx, node)
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
func TestSchedule_Errors_WhenInformationMissingFromState(t *testing.T) {
	//"will error if scheduled job cannot be found in jobDb": {
	//expectSuccess: false,
	//},
	//"will error if scheduled job cannot be found in scheduling context": {
	//expectSuccess: false,
	//},
	//"will error if scheduled job is not in successfully scheduled jobs in scheduling context": {
	//expectSuccess: false,
	//},
	//"will error if scheduled job does not have a pod scheduling context": {
	//expectSuccess: false,
	//},
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
