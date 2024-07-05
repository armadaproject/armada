package scheduler

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestSchedule(t *testing.T) {
	type scheduledJobs struct {
		jobs         []*jobdb.Job
		acknowledged bool
	}
	tests := map[string]struct {
		schedulingConfig configuration.SchedulingConfig

		executors  []*schedulerobjects.Executor
		queues     []*api.Queue
		queuedJobs []*jobdb.Job

		// Already scheduled jobs. Specifically,
		// [executorIndex][nodeIndex] = jobs scheduled onto this executor and node,
		// where executorIndex refers to the index of executors, and nodeIndex the index of the node on that executor.
		scheduledJobsByExecutorIndexAndNodeIndex map[int]map[int]scheduledJobs

		// Indices of existing jobs expected to be preempted.
		// Uses the same structure as scheduledJobsByExecutorIndexAndNodeIndex.
		expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex map[int]map[int][]int

		// Indices of queued jobs expected to be scheduled.
		expectedScheduledIndices []int
		// Number of jobs expected to be scheduled by pool
		expectedScheduledByPool map[string]int
	}{
		"scheduling": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1, 2, 3},
			expectedScheduledByPool:  map[string]int{testfixtures.TestPool: 4},
		},
		"scheduling - mixed pool clusters": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.MakeTestExecutor("executor-1", "pool-1", "pool-2"),
				testfixtures.MakeTestExecutor("executor-2", "pool-1"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1, 2, 3, 4, 5},
			expectedScheduledByPool:  map[string]int{"pool-1": 4, "pool-2": 2},
		},
		"Fair share": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*api.Queue{
				{
					Name:           "testQueueA",
					PriorityFactor: 100,
				},
				{
					Name:           "testQueueB",
					PriorityFactor: 300,
				},
			},
			queuedJobs: append(
				testfixtures.N16Cpu128GiJobs("testQueueA", testfixtures.PriorityClass3, 10),
				testfixtures.N16Cpu128GiJobs("testQueueB", testfixtures.PriorityClass3, 10)...,
			),
			expectedScheduledIndices: []int{0, 1, 2, 10},
		},
		"do not schedule onto stale executors": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.WithLastUpdateTimeExecutor(testfixtures.BaseTime.Add(-1*time.Hour), testfixtures.Test1Node32CoreExecutor("executor2")),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1},
		},
		"schedule onto executors with some unacknowledged jobs": {
			schedulingConfig: testfixtures.WithMaxUnacknowledgedJobsPerExecutorConfig(16, testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 48),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 16),
						acknowledged: false,
					},
				},
			},
			expectedScheduledIndices: testfixtures.IntRange(0, 47),
		},
		"do not schedule onto executors with too many unacknowledged jobs": {
			schedulingConfig: testfixtures.WithMaxUnacknowledgedJobsPerExecutorConfig(15, testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 48),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 16),
						acknowledged: false,
					},
				},
			},
			expectedScheduledIndices: testfixtures.IntRange(0, 31),
		},
		"one executor full": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 2),
						acknowledged: true,
					},
				},
			},
			expectedScheduledIndices: []int{0, 1},
		},
		"MaximumResourceFractionPerQueue hit before scheduling": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass3: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 2),
						acknowledged: true,
					},
				},
			},
		},
		"MaximumResourceFractionPerQueue hit during scheduling": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass3: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 1),
						acknowledged: true,
					},
				},
			},
			expectedScheduledIndices: []int{0},
		},
		"no queued jobs": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
		},
		"no executors": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{},
			queues:           []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:       testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
		},
		"computation of allocated resources does not confuse priority class with per-queue priority": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass3: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),
			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: []*jobdb.Job{
				// Submit the next job with a per-queue priority number (i.e., 1) that is larger
				// than the per-queue priority of the already-running job (i.e., 0), but smaller
				// than the priority class number of the two jobs (i.e., 3); if the scheduler were
				// to use the per-queue priority instead of the priority class number in its
				// accounting, then it would schedule this job.
				testfixtures.Test16Cpu128GiJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithPriority(1),
			},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         []*jobdb.Job{testfixtures.Test16Cpu128GiJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithPriority(0)},
						acknowledged: true,
					},
				},
			},
		},
		"urgency-based preemption within a single queue": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:           []*api.Queue{{Name: "A"}},
			queuedJobs:       testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass1, 2),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 1),
						acknowledged: true,
					},
				},
			},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedScheduledIndices: []int{0, 1},
		},
		"urgency-based preemption between queues": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:           []*api.Queue{{Name: "A"}, {Name: "B"}},
			queuedJobs:       testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass1, 2),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass0, 1),
						acknowledged: true,
					},
				},
			},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedScheduledIndices: []int{0, 1},
		},
		"preemption to fair share": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:           []*api.Queue{{Name: "A", PriorityFactor: 0.01}, {Name: "B", PriorityFactor: 0.01}},
			queuedJobs:       testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 2),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass0, 2),
						acknowledged: true,
					},
				},
			},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {1},
				},
			},
			expectedScheduledIndices: []int{0},
		},
		"gang scheduling successful": {
			schedulingConfig:         testfixtures.TestSchedulingConfig(),
			executors:                []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 2)),
			expectedScheduledIndices: []int{0, 1},
		},
		"gang scheduling successful - mixed pool clusters": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.MakeTestExecutor("executor1", "pool-1", "pool-2"),
				testfixtures.MakeTestExecutor("executor2", "pool-1"),
			},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.PoolNameLabel),
			expectedScheduledIndices: []int{0, 1, 2},
			expectedScheduledByPool:  map[string]int{"pool-1": 3},
		},
		"not scheduling a gang that does not fit on any executor": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs: testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.ClusterNameLabel),
		},
		"not scheduling a gang that does not fit on any pool": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{testfixtures.MakeTestExecutor("executor1", "pool-1", "pool-2")},
			queues:           []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:       testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.ClusterNameLabel),
		},
		"urgency-based gang preemption": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
			},
			queues:     []*api.Queue{{Name: "queue1", PriorityFactor: 0.01}, {Name: "queue2", PriorityFactor: 0.01}},
			queuedJobs: testfixtures.N16Cpu128GiJobs("queue2", testfixtures.PriorityClass1, 1),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("queue1", testfixtures.PriorityClass0, 2)),
						acknowledged: true,
					},
				},
			},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0, 1},
				},
			},
			expectedScheduledIndices: []int{0},
		},
		"preemption to fair share evicting a gang": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:           []*api.Queue{{Name: "queue1", PriorityFactor: 0.01}, {Name: "queue2", PriorityFactor: 0.01}},
			queuedJobs:       testfixtures.N16Cpu128GiJobs("queue2", testfixtures.PriorityClass0, 1),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("queue1", testfixtures.PriorityClass0, 2)),
						acknowledged: true,
					},
				},
			},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0, 1},
				},
			},
			expectedScheduledIndices: []int{0},
		},
		"Schedule gang job over multiple executors": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass0, 4)),
			expectedScheduledIndices: []int{0, 1, 2, 3},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			mockQueueCache.EXPECT().GetAll(ctx).Return(tc.queues, nil).AnyTimes()

			schedulingContextRepo := reports.NewSchedulingContextRepository()
			sch, err := NewFairSchedulingAlgo(
				tc.schedulingConfig,
				0,
				mockExecutorRepo,
				mockQueueCache,
				schedulingContextRepo,
				nil,
				nil,
				stringinterner.New(1024),
				testfixtures.TestResourceListFactory,
				testfixtures.TestEmptyFloatingResources,
			)
			require.NoError(t, err)

			// Use a test clock so we can control time
			sch.clock = clock.NewFakeClock(testfixtures.BaseTime)

			// Add queued jobs to the jobDb.
			jobsToUpsert := make([]*jobdb.Job, 0)
			queueIndexByJobId := make(map[string]int)
			for i, job := range tc.queuedJobs {
				job = job.WithQueued(true)
				jobsToUpsert = append(jobsToUpsert, job)
				queueIndexByJobId[job.Id()] = i
			}

			// Add scheduled jobs to the jobDb. Bind acknowledged jobs to nodes.
			executorIndexByJobId := make(map[string]int)
			executorNodeIndexByJobId := make(map[string]int)
			jobIndexByJobId := make(map[string]int)
			for executorIndex, existingJobsByExecutorNodeIndex := range tc.scheduledJobsByExecutorIndexAndNodeIndex {
				executor := tc.executors[executorIndex]
				for nodeIndex, existingJobs := range existingJobsByExecutorNodeIndex {
					node := executor.Nodes[nodeIndex]
					for jobIndex, job := range existingJobs.jobs {
						job = job.WithQueued(false).WithNewRun(executor.Id, node.Id, node.Name, node.Pool, job.PodRequirements().Priority)
						if existingJobs.acknowledged {
							run := job.LatestRun()
							node.StateByJobRunId[run.Id().String()] = schedulerobjects.JobRunState_RUNNING
						}
						jobsToUpsert = append(jobsToUpsert, job)
						executorIndexByJobId[job.Id()] = executorIndex
						executorNodeIndexByJobId[job.Id()] = nodeIndex
						jobIndexByJobId[job.Id()] = jobIndex
					}
				}
			}

			// Setup jobDb.
			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err = txn.Upsert(jobsToUpsert)
			require.NoError(t, err)

			// Run a scheduling round.
			schedulerResult, err := sch.Schedule(ctx, txn)
			require.NoError(t, err)

			// Check that the expected preemptions took place.
			preemptedJobs := PreemptedJobsFromSchedulerResult(schedulerResult)
			actualPreemptedJobsByExecutorIndexAndNodeIndex := make(map[int]map[int][]int)
			for _, job := range preemptedJobs {
				executorIndex := executorIndexByJobId[job.Id()]
				nodeIndex := executorNodeIndexByJobId[job.Id()]
				jobIndex := jobIndexByJobId[job.Id()]
				m := actualPreemptedJobsByExecutorIndexAndNodeIndex[executorIndex]
				if m == nil {
					m = make(map[int][]int)
					actualPreemptedJobsByExecutorIndexAndNodeIndex[executorIndex] = m
				}
				m[nodeIndex] = append(m[nodeIndex], jobIndex)
			}
			for _, m := range actualPreemptedJobsByExecutorIndexAndNodeIndex {
				for _, s := range m {
					slices.Sort(s)
				}
			}
			if len(tc.expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex) == 0 {
				assert.Equal(t, 0, len(actualPreemptedJobsByExecutorIndexAndNodeIndex))
			} else {
				assert.Equal(t, tc.expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex, actualPreemptedJobsByExecutorIndexAndNodeIndex)
			}

			// Check that jobs were scheduled as expected.
			scheduledJobs := ScheduledJobsFromSchedulerResult(schedulerResult)
			actualScheduledIndices := make([]int, 0)
			for _, job := range scheduledJobs {
				actualScheduledIndices = append(actualScheduledIndices, queueIndexByJobId[job.Id()])
			}
			slices.Sort(actualScheduledIndices)
			if len(tc.expectedScheduledIndices) == 0 {
				assert.Equal(t, 0, len(actualScheduledIndices))
			} else {
				assert.Equal(t, tc.expectedScheduledIndices, actualScheduledIndices)
			}

			scheduledJobsPerPool := armadaslices.GroupByFunc(scheduledJobs, func(j *jobdb.Job) string {
				return j.LatestRun().Pool()
			})
			for pool, expectedScheduledCount := range tc.expectedScheduledByPool {
				jobsSchedulerOnPool, present := scheduledJobsPerPool[pool]
				assert.True(t, present)
				assert.Len(t, jobsSchedulerOnPool, expectedScheduledCount)
			}

			// Check that preempted jobs are marked as such consistently.
			for _, job := range preemptedJobs {
				dbJob := txn.GetById(job.Id())
				assert.True(t, dbJob.Failed())
				assert.False(t, dbJob.Queued())
			}

			// Check that scheduled jobs are marked as such consistently.
			for _, job := range scheduledJobs {
				dbJob := txn.GetById(job.Id())
				assert.False(t, dbJob.Failed())
				assert.False(t, dbJob.Queued())
				dbRun := dbJob.LatestRun()
				assert.False(t, dbRun.Failed())
				assert.Equal(t, schedulerResult.NodeIdByJobId[dbJob.Id()], dbRun.NodeId())
				assert.NotEmpty(t, dbRun.NodeName())
			}

			// Check that jobDb was updated correctly.
			// TODO: Check that there are no unexpected jobs in the jobDb.
			for _, job := range preemptedJobs {
				dbJob := txn.GetById(job.Id())
				assert.True(t, job.Equal(dbJob), "expected %v but got %v", job, dbJob)
			}
			for _, job := range scheduledJobs {
				dbJob := txn.GetById(job.Id())
				assert.True(t, job.Equal(dbJob), "expected %v but got %v", job, dbJob)
			}

			// Check that we calculated fair share and adjusted fair share
			for _, schCtx := range schedulerResult.SchedulingContexts {
				for _, qtx := range schCtx.QueueSchedulingContexts {
					assert.NotEqual(t, 0, qtx.AdjustedFairShare)
					assert.NotEqual(t, 0, qtx.FairShare)
				}
			}
		})
	}
}

func BenchmarkNodeDbConstruction(b *testing.B) {
	for e := 1; e <= 4; e++ {
		numNodes := int(math.Pow10(e))
		b.Run(fmt.Sprintf("%d nodes", numNodes), func(b *testing.B) {
			jobs := testfixtures.N1Cpu4GiJobs("queue-alice", testfixtures.PriorityClass0, 32*numNodes)
			nodes := testfixtures.N32CpuNodes(numNodes, testfixtures.TestPriorities)
			for i, node := range nodes {
				for j := 32 * i; j < 32*(i+1); j++ {
					jobs[j] = jobs[j].WithNewRun("executor-01", node.Id, node.Name, node.Pool, jobs[j].PodRequirements().Priority)
				}
			}
			armadaslices.Shuffle(jobs)
			schedulingConfig := testfixtures.TestSchedulingConfig()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				stringInterner := stringinterner.New(1024)
				algo, err := NewFairSchedulingAlgo(
					schedulingConfig,
					time.Second*5,
					nil,
					nil,
					nil,
					nil,
					nil,
					stringInterner,
					testfixtures.TestResourceListFactory,
					testfixtures.TestEmptyFloatingResources,
				)
				require.NoError(b, err)
				b.StartTimer()

				nodeDb, err := nodedb.NewNodeDb(
					schedulingConfig.PriorityClasses,
					schedulingConfig.IndexedResources,
					schedulingConfig.IndexedTaints,
					schedulingConfig.IndexedNodeLabels,
					schedulingConfig.WellKnownNodeTypes,
					stringInterner,
					testfixtures.TestResourceListFactory,
				)
				require.NoError(b, err)
				err = algo.populateNodeDb(nodeDb, jobs, nodes)
				require.NoError(b, err)
			}
		})
	}
}

func TestSortExecutorGroups(t *testing.T) {
	tests := map[string]struct {
		groups          []string
		groupToPriority map[string]int
		defaultPriority int
		expected        []string
	}{
		"All groups have defined priority": {
			groups:          []string{"pool1", "pool2", "pool3"},
			groupToPriority: map[string]int{"pool1": 3, "pool2": 1, "pool3": 2},
			expected:        []string{"pool1", "pool3", "pool2"},
		},
		"Use default Priority": {
			groups:          []string{"pool1", "pool2", "pool3"},
			groupToPriority: map[string]int{"pool1": 3, "pool2": 1},
			defaultPriority: 2,
			expected:        []string{"pool1", "pool3", "pool2"},
		},
		"Tie break on alphabetical": {
			groups:          []string{"pool1", "pool2", "pool3"},
			groupToPriority: map[string]int{"pool1": 2, "pool2": 2, "pool3": 3},
			defaultPriority: 2,
			expected:        []string{"pool3", "pool1", "pool2"},
		},
		"Empty priority yields alphabetical": {
			groups:   []string{"pool2", "pool3", "pool1"},
			expected: []string{"pool1", "pool2", "pool3"},
		},
		"Empty input": {
			groups:   []string{},
			expected: []string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sortGroups(tc.groups, tc.groupToPriority, tc.defaultPriority)
			assert.Equal(t, tc.expected, tc.groups)
		})
	}
}
