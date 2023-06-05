package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestLegacySchedulingAlgo_TestSchedule(t *testing.T) {
	tests := map[string]struct {
		schedulingConfig configuration.SchedulingConfig

		executors []*schedulerobjects.Executor
		queues    []*database.Queue

		existingJobs []*jobdb.Job
		// Map from (executor ID, node name) to indices of acknowledged jobs for that node.
		existingRunningIndices map[string]map[string][]int
		// Map from (executor ID, node name) to indices of unacknowledged jobs for that node.
		existingUnacknowledgedIndices map[string]map[string][]int

		queuedJobs []*jobdb.Job

		// Indices of jobs that we expect to be preempted.
		expectedPreemptedIndices []int
		// Map from executor ID to indices of jobs that we expect to be scheduled.
		expectedScheduledIndices map[string][]int
	}{
		"fill up both clusters": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
				"executor2": {2, 3},
			},
		},
		"one executor stale": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.WithLastUpdateTimeExecutor(testfixtures.BaseTime.Add(-1*time.Hour), testfixtures.Test1Node32CoreExecutor("executor2")),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
			},
		},
		"one executor exceeds unacknowledged": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			existingJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 2),
			existingUnacknowledgedIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),

			expectedScheduledIndices: map[string][]int{
				"executor2": {0, 1},
			},
		},
		"one executor full": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			existingJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 2),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),

			expectedScheduledIndices: map[string][]int{
				"executor2": {0, 1},
			},
		},
		"user is at usage cap before scheduling": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			existingJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 2),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),

			expectedScheduledIndices: nil,
		},
		"user hits usage cap during scheduling": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			existingJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 1),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0}},
			},

			queuedJobs: testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),

			expectedScheduledIndices: map[string][]int{
				"executor1": {0},
			},
		},
		"no queuedJobs to schedule": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{testfixtures.TestDbQueue()},

			expectedScheduledIndices: nil,
		},
		"no executor available": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{},
			queues:    []*database.Queue{testfixtures.TestDbQueue()},

			queuedJobs:               testfixtures.N16CpuJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: nil,
		},
		"computation of allocated resources does not confuse priority class with per-queue priority": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[int32]map[string]float64{
					testfixtures.TestPriorityClasses[testfixtures.PriorityClass3].Priority: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{testfixtures.TestDbQueue()},

			existingJobs: []*jobdb.Job{testfixtures.Test16CpuJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithPriority(0)},
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0}},
			},

			queuedJobs: []*jobdb.Job{
				// Submit the next job with a per-queue priority number (i.e., 1) that is larger
				// than the per-queue priority of the already-running job (i.e., 0), but smaller
				// than the priority class number of the two jobs (i.e., 3); if the scheduler were
				// to use the per-queue priority instead of the priority class number in its
				// accounting, then it would schedule this job.
				testfixtures.Test16CpuJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithPriority(1),
			},

			expectedScheduledIndices: nil,
		},
		"urgency-based preemption within a single queue": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{{Name: "queue1", Weight: 100}},

			existingJobs: testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass1, 2),

			expectedPreemptedIndices: []int{0, 1},
			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
			},
		},
		"urgency-based preemption across queues": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{{Name: "queue1", Weight: 100}, {Name: "queue2", Weight: 100}},

			existingJobs: testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs("queue2", testfixtures.PriorityClass1, 2),

			expectedPreemptedIndices: []int{0, 1},
			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
			},
		},
		"preemption to fair share": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{{Name: "queue1", Weight: 100}, {Name: "queue2", Weight: 100}},

			existingJobs: testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs("queue2", testfixtures.PriorityClass0, 2),

			expectedPreemptedIndices: []int{1},
			expectedScheduledIndices: map[string][]int{
				"executor1": {0},
			},
		},
		"scheduling a gang": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{{Name: "queue1", Weight: 100}},

			queuedJobs: testfixtures.WithGangAnnotationsJobs(testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2)),

			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
			},
		},
		"not scheduling a gang that is too large for any of the executors": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{{Name: "queue1", Weight: 100}},

			queuedJobs: testfixtures.WithGangAnnotationsJobs(testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 3)),

			expectedScheduledIndices: nil,
		},
		"urgency-based preemption evicting a gang": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1"),
				testfixtures.Test1Node32CoreExecutor("executor2"),
			},
			queues: []*database.Queue{{Name: "queue1", Weight: 100}, {Name: "queue2", Weight: 100}},

			existingJobs: testfixtures.WithGangAnnotationsJobs(testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2)),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs("queue2", testfixtures.PriorityClass1, 4),

			expectedPreemptedIndices: []int{0, 1},
			expectedScheduledIndices: map[string][]int{
				"executor1": {0, 1},
				"executor2": {2, 3},
			},
		},
		"preemption to fair share evicting a gang": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),

			executors: []*schedulerobjects.Executor{testfixtures.Test1Node32CoreExecutor("executor1")},
			queues:    []*database.Queue{{Name: "queue1", Weight: 100}, {Name: "queue2", Weight: 100}},

			existingJobs: testfixtures.WithGangAnnotationsJobs(testfixtures.N16CpuJobs("queue1", testfixtures.PriorityClass0, 2)),
			existingRunningIndices: map[string]map[string][]int{
				"executor1": {"executor1-node": {0, 1}},
			},

			queuedJobs: testfixtures.N16CpuJobs("queue2", testfixtures.PriorityClass0, 1),

			expectedPreemptedIndices: []int{0, 1},
			expectedScheduledIndices: map[string][]int{
				"executor1": {0},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := testfixtures.ContextWithDefaultLogger(context.Background())
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			ctrl := gomock.NewController(t)

			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()

			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)
			mockQueueRepo.EXPECT().GetAllQueues().Return(tc.queues, nil).AnyTimes()

			algo, err := NewFairSchedulingAlgo(
				tc.schedulingConfig,
				time.Second*5,
				mockExecutorRepo,
				mockQueueRepo,
			)
			require.NoError(t, err)

			// Use a test clock so we can control time
			algo.clock = clock.NewFakeClock(testfixtures.BaseTime)

			nodes := make(map[string]map[string]*schedulerobjects.Node)
			for _, executor := range tc.executors {
				nodesByName := make(map[string]*schedulerobjects.Node)
				for _, node := range executor.Nodes {
					nodesByName[node.Name] = node
				}
				nodes[executor.Id] = nodesByName
			}

			jobsToUpsert := make([]*jobdb.Job, 0)

			for executorId, jobsByNodeName := range tc.existingRunningIndices {
				for nodeName, jobIndices := range jobsByNodeName {
					node := nodes[executorId][nodeName]

					for _, i := range jobIndices {
						job := tc.existingJobs[i].WithQueued(false).WithNewRun(executorId, nodeName)
						jobsToUpsert = append(jobsToUpsert, job)
						run := job.LatestRun()
						node.StateByJobRunId[run.Id().String()] = schedulerobjects.JobRunState_RUNNING

						req := PodRequirementFromLegacySchedulerJob(job, tc.schedulingConfig.Preemption.PriorityClasses)
						node, err = nodedb.BindPodToNode(req, node)
						require.NoError(t, err)
					}

					nodes[executorId][nodeName] = node
				}
			}

			for executorId, jobsByNodeName := range tc.existingUnacknowledgedIndices {
				for nodeName, jobIndices := range jobsByNodeName {
					for _, i := range jobIndices {
						job := tc.existingJobs[i].WithQueued(false).WithNewRun(executorId, nodeName)
						jobsToUpsert = append(jobsToUpsert, job)
					}
				}
			}

			for _, job := range tc.queuedJobs {
				job = job.WithQueued(true)
				jobsToUpsert = append(jobsToUpsert, job)
			}

			jobDb := jobdb.NewJobDb()
			txn := jobDb.WriteTxn()
			err = jobDb.Upsert(txn, jobsToUpsert)
			require.NoError(t, err)

			schedulerResult, err := algo.Schedule(ctx, txn, jobDb)
			require.NoError(t, err)

			expectedScheduledJobs := make(map[string]string)
			for executorId, jobIndices := range tc.expectedScheduledIndices {
				for _, i := range jobIndices {
					expectedScheduledJobs[tc.queuedJobs[i].Id()] = executorId
				}
			}

			assert.Equal(t, len(expectedScheduledJobs), len(schedulerResult.ScheduledJobs))

			scheduledJobs := ScheduledJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
			for _, job := range scheduledJobs {
				assert.Equal(t, false, job.Queued())
				expectedExecutor, ok := expectedScheduledJobs[job.Id()]
				require.True(t, ok)
				run := job.LatestRun()
				require.NotNil(t, run)
				assert.Equal(t, expectedExecutor, run.Executor())
			}

			// check all scheduled queuedJobs are up-to-date in db
			for _, job := range scheduledJobs {
				dbJob := jobDb.GetById(txn, job.Id())
				require.NoError(t, err)
				assert.Equal(t, job, dbJob)
			}

			expectedPreemptedJobs := make([]string, 0)
			for _, i := range tc.expectedPreemptedIndices {
				expectedPreemptedJobs = append(expectedPreemptedJobs, tc.existingJobs[i].Id())
			}
			slices.Sort(expectedPreemptedJobs)
			preemptedJobs := make([]string, 0)
			for _, job := range PreemptedJobsFromSchedulerResult[*jobdb.Job](schedulerResult) {
				preemptedJobs = append(preemptedJobs, job.Id())
			}
			slices.Sort(preemptedJobs)
			assert.Equal(t, expectedPreemptedJobs, preemptedJobs)
		})
	}
}

func TestGetExecutorsToSchedule(t *testing.T) {
	executorA := testfixtures.Test1Node32CoreExecutor("a")
	executorA1 := testfixtures.Test1Node32CoreExecutor("a1")
	executorB := testfixtures.Test1Node32CoreExecutor("b")
	executorC := testfixtures.Test1Node32CoreExecutor("c")

	tests := map[string]struct {
		executors          []*schedulerobjects.Executor
		expectedExecutors  []*schedulerobjects.Executor
		previousExecutorId string
	}{
		"sorts executors lexographically": {
			executors: []*schedulerobjects.Executor{
				executorB,
				executorA,
				executorA1,
			},
			expectedExecutors: []*schedulerobjects.Executor{
				executorA,
				executorA1,
				executorB,
			},
			previousExecutorId: "",
		},
		"adjusts order based on previous executor id": {
			executors: []*schedulerobjects.Executor{
				executorC,
				executorB,
				executorA,
			},
			expectedExecutors: []*schedulerobjects.Executor{
				executorB,
				executorC,
				executorA,
			},
			previousExecutorId: "a",
		},
		"previous executor id greater than any known executor": {
			executors: []*schedulerobjects.Executor{
				executorC,
				executorA,
				executorB,
			},
			expectedExecutors: []*schedulerobjects.Executor{
				executorA,
				executorB,
				executorC,
			},
			previousExecutorId: "d",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			schedulingAlgoContext := fairSchedulingAlgoContext{executors: tc.executors}
			result := schedulingAlgoContext.getExecutorsToSchedule(tc.previousExecutorId)
			assert.Equal(t, tc.expectedExecutors, result)
		})
	}
}

type executorOrderingTest struct {
	executors                           []*schedulerobjects.Executor
	expectedExecutorsScheduled          []string
	expectedPreviousScheduledExecutorId string
}

func TestLegacySchedulingAlgo_TestSchedule_ExecutorOrdering(t *testing.T) {
	tests := map[string]struct {
		onExecutorScheduled func(executor *schedulerobjects.Executor)
		maxScheduleDuration time.Duration
		rounds              []executorOrderingTest
	}{
		"considers all executors in order": {
			onExecutorScheduled: func(executor *schedulerobjects.Executor) {},
			maxScheduleDuration: time.Second * 1,
			rounds: []executorOrderingTest{
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor2"),
					},
					expectedExecutorsScheduled:          []string{"executor1", "executor2"},
					expectedPreviousScheduledExecutorId: "executor2",
				},
			},
		},
		"maintains state between schedule calls": {
			onExecutorScheduled: func(executor *schedulerobjects.Executor) { time.Sleep(time.Millisecond * 200) },
			maxScheduleDuration: time.Millisecond * 100,
			rounds: []executorOrderingTest{
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor2"),
					},
					expectedExecutorsScheduled:          []string{"executor1"},
					expectedPreviousScheduledExecutorId: "executor1",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor2"),
					},
					expectedExecutorsScheduled:          []string{"executor2"},
					expectedPreviousScheduledExecutorId: "executor2",
				},
			},
		},
		"handles executors changing between schedule calls": {
			onExecutorScheduled: func(executor *schedulerobjects.Executor) { time.Sleep(time.Millisecond * 200) },
			maxScheduleDuration: time.Millisecond * 100,
			rounds: []executorOrderingTest{
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor3"),
					},
					expectedExecutorsScheduled:          []string{"executor1"},
					expectedPreviousScheduledExecutorId: "executor1",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor2"),
						testfixtures.Test1Node32CoreExecutor("executor3"),
					},
					expectedExecutorsScheduled:          []string{"executor2"},
					expectedPreviousScheduledExecutorId: "executor2",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1"),
						testfixtures.Test1Node32CoreExecutor("executor2"),
					},
					expectedExecutorsScheduled:          []string{"executor1"},
					expectedPreviousScheduledExecutorId: "executor1",
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := testfixtures.ContextWithDefaultLogger(context.Background())
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			config := testfixtures.TestSchedulingConfig()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)
			mockQueueRepo.EXPECT().GetAllQueues().Return([]*database.Queue{}, nil).AnyTimes()

			algo, err := NewFairSchedulingAlgo(
				config,
				tc.maxScheduleDuration,
				mockExecutorRepo,
				mockQueueRepo,
			)
			require.NoError(t, err)
			scheduledExecutorsIds := []string{}
			// Use a test clock so we can control time
			algo.clock = clock.NewFakeClock(testfixtures.BaseTime)
			algo.onExecutorScheduled = func(executor *schedulerobjects.Executor) {
				scheduledExecutorsIds = append(scheduledExecutorsIds, executor.Id)
				tc.onExecutorScheduled(executor)
			}

			// Set up JobDb
			jobDb := jobdb.NewJobDb()

			txn := jobDb.WriteTxn()
			for _, round := range tc.rounds {
				scheduledExecutorsIds = []string{}

				roundMockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
				roundMockExecutorRepo.EXPECT().GetExecutors(ctx).Return(round.executors, nil).AnyTimes()
				algo.executorRepository = roundMockExecutorRepo

				_, err := algo.Schedule(ctx, txn, jobDb)
				require.NoError(t, err)
				assert.Equal(t, scheduledExecutorsIds, round.expectedExecutorsScheduled)
				assert.Equal(t, round.expectedPreviousScheduledExecutorId, algo.previousScheduleClusterId)
			}
		})
	}
}
