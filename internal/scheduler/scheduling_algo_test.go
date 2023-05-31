package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestLegacySchedulingAlgo_TestSchedule(t *testing.T) {
	queuedJobs := make([]*jobdb.Job, 10)
	for i := 0; i < 10; i++ {
		queuedJobs[i] = testfixtures.Test16CpuJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithQueued(true)
	}
	runningJobs := []*jobdb.Job{
		testfixtures.Test16CpuJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithQueued(false).WithNewRun("executor1", "executor1-node"),
		testfixtures.Test16CpuJob(testfixtures.TestQueue, testfixtures.PriorityClass3).WithQueued(false).WithNewRun("executor1", "executor1-node"),
	}
	tests := map[string]struct {
		executors                        []*schedulerobjects.Executor
		queues                           []*database.Queue
		queuedJobs                       []*jobdb.Job
		runningJobs                      []*jobdb.Job
		unacknowledgedJobs               []*jobdb.Job
		perQueueLimit                    map[string]float64
		maxUnacknowledgedJobsPerExecutor uint
		expectedJobs                     map[string]string // map of jobId to name of executor on which it should be scheduled
	}{
		"fill up both clusters": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:     []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs: queuedJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
				queuedJobs[1].Id(): "executor1",
				queuedJobs[2].Id(): "executor2",
				queuedJobs[3].Id(): "executor2",
			},
		},
		"one executor stale": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime.Add(-1*time.Hour)),
			},
			queues:     []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs: queuedJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
				queuedJobs[1].Id(): "executor1",
			},
		},
		"one executor exceeds unacknowledged": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:             []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs:         queuedJobs,
			unacknowledgedJobs: runningJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor2",
				queuedJobs[1].Id(): "executor2",
			},
		},
		"one executor full": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:      []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs:  queuedJobs,
			runningJobs: runningJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor2",
				queuedJobs[1].Id(): "executor2",
			},
		},
		"user is at usage cap before scheduling": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:        []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs:    queuedJobs,
			runningJobs:   []*jobdb.Job{runningJobs[0], runningJobs[1]},
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs:  map[string]string{},
		},
		"user hits usage cap during scheduling": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:                           []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs:                       queuedJobs,
			runningJobs:                      []*jobdb.Job{runningJobs[0]},
			perQueueLimit:                    map[string]float64{"cpu": 0.5},
			maxUnacknowledgedJobsPerExecutor: 1,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
			},
		},
		"no queuedJobs to schedule": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
				testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
			},
			queues:       []*database.Queue{testfixtures.TestDbQueue()},
			expectedJobs: map[string]string{},
		},
		"no executor available": {
			executors:    []*schedulerobjects.Executor{},
			queues:       []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs:   queuedJobs,
			expectedJobs: map[string]string{},
		},
		"The scheduling algorithm computes allocated resources by priority class, not by per-queue priority.": {
			executors: []*schedulerobjects.Executor{
				testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
			},
			runningJobs: []*jobdb.Job{runningJobs[0].WithPriority(0)},
			queues:      []*database.Queue{testfixtures.TestDbQueue()},
			queuedJobs: []*jobdb.Job{
				// Submit the next job with a per-queue priority number (i.e., 1) that is larger
				// than the per-queue priority of the already-running job (i.e., 0), but smaller
				// than the priority class number of the two jobs (i.e., 3); if the scheduler were
				// to use the per-queue priority instead of the priority class number in its
				// accounting, then it would schedule this job.
				queuedJobs[0].WithPriority(1),
			},
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs:  map[string]string{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := testfixtures.ContextWithDefaultLogger(context.Background())
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			config := testfixtures.TestSchedulingConfig()
			if tc.perQueueLimit != nil {
				priorityClass := testfixtures.TestPriorityClasses[testfixtures.PriorityClass3]
				config = testfixtures.WithPerPriorityLimitsConfig(map[int32]map[string]float64{priorityClass.Priority: tc.perQueueLimit}, config)
			}
			if tc.maxUnacknowledgedJobsPerExecutor != 0 {
				config = testfixtures.WithMaxUnacknowledgedJobsPerExecutor(tc.maxUnacknowledgedJobsPerExecutor, config)
			}
			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)

			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			mockQueueRepo.EXPECT().GetAllQueues().Return(tc.queues, nil).AnyTimes()

			algo, err := NewFairSchedulingAlgo(
				config,
				time.Second*5,
				mockExecutorRepo,
				mockQueueRepo,
			)
			require.NoError(t, err)

			// Use a test clock so we can control time
			algo.clock = clock.NewFakeClock(testfixtures.BaseTime)

			// Set up JobDb
			jobDb := jobdb.NewJobDb()

			txn := jobDb.WriteTxn()
			err = jobDb.Upsert(txn, tc.queuedJobs)
			require.NoError(t, err)

			err = jobDb.Upsert(txn, tc.unacknowledgedJobs)
			require.NoError(t, err)

			executorsById := make(map[string]*schedulerobjects.Executor, len(tc.executors))
			for _, executor := range tc.executors {
				executorsById[executor.Id] = executor
			}
			for _, job := range tc.runningJobs {
				run := job.LatestRun()
				executor := executorsById[run.Executor()]
				executor.UnassignedJobRuns = append(executor.UnassignedJobRuns, run.Id().String())
			}
			err = jobDb.Upsert(txn, tc.runningJobs)
			require.NoError(t, err)

			schedulerResult, err := algo.Schedule(ctx, txn, jobDb)
			require.NoError(t, err)

			// check that we have scheduled the queuedJobs we expect
			assert.Equal(t, len(tc.expectedJobs), len(schedulerResult.ScheduledJobs))

			scheduledJobs := ScheduledJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
			for _, job := range scheduledJobs {
				expectedExecutor, ok := tc.expectedJobs[job.Id()]
				require.True(t, ok)
				run := job.LatestRun()
				require.NotEqual(t, t, run)
				assert.Equal(t, expectedExecutor, run.Executor())
				assert.Equal(t, false, job.Queued())
			}

			// check all scheduled queuedJobs are up-to-date in db
			for _, job := range scheduledJobs {
				dbJob := jobDb.GetById(txn, job.Id())
				require.NoError(t, err)
				assert.Equal(t, job, dbJob)
			}
		})
	}
}

func TestGetExecutorsToSchedule(t *testing.T) {
	executorA := testfixtures.Test1Node32CoreExecutor("a", testfixtures.BaseTime)
	executorA1 := testfixtures.Test1Node32CoreExecutor("a1", testfixtures.BaseTime)
	executorB := testfixtures.Test1Node32CoreExecutor("b", testfixtures.BaseTime)
	executorC := testfixtures.Test1Node32CoreExecutor("c", testfixtures.BaseTime)

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
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
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
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
					},
					expectedExecutorsScheduled:          []string{"executor1"},
					expectedPreviousScheduledExecutorId: "executor1",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
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
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor3", testfixtures.BaseTime),
					},
					expectedExecutorsScheduled:          []string{"executor1"},
					expectedPreviousScheduledExecutorId: "executor1",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor3", testfixtures.BaseTime),
					},
					expectedExecutorsScheduled:          []string{"executor2"},
					expectedPreviousScheduledExecutorId: "executor2",
				},
				{
					executors: []*schedulerobjects.Executor{
						testfixtures.Test1Node32CoreExecutor("executor1", testfixtures.BaseTime),
						testfixtures.Test1Node32CoreExecutor("executor2", testfixtures.BaseTime),
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
