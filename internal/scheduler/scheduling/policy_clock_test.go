package scheduling

import (
	"context"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/priorityoverride"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestFairSchedulingAlgoPolicyClockRefillsGlobalLimiter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		advance  time.Duration
		expected int
	}{
		{"full refill", 20 * time.Second, 1},
		{"insufficient refill", 5 * time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testGlobalRate(t, tc.advance, tc.expected)
		})
	}
}

func testGlobalRate(t *testing.T, advance time.Duration, expected int) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	executors := []*schedulerobjects.Executor{makeTestExecutor("clock-test", testfixtures.TestPool)}
	repo := schedulermocks.NewMockExecutorRepository(ctrl)
	repo.EXPECT().GetExecutors(gomock.Any()).Return(executors, nil).AnyTimes()
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
	queues := schedulermocks.NewMockQueueCache(ctrl)
	queues.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{testfixtures.MakeTestQueue()}, nil).AnyTimes()
	cfg := testfixtures.TestSchedulingConfig()
	cfg.MaximumSchedulingRate = 0.1
	cfg.MaximumSchedulingBurst = 1
	clk := clocktesting.NewFakeClock(testfixtures.BaseTime)
	sch, err := NewFairSchedulingAlgo(cfg, 0, repo, queues, reports.NewSchedulingContextRepository(), testfixtures.TestResourceListFactory, testfixtures.TestEmptyFloatingResources, priorityoverride.NewNoOpProvider(), nil, &testRunReconciler{}, WithPolicyClock(clk))
	require.NoError(t, err)
	db := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	db.SetClock(clk)
	txn := db.WriteTxn()
	defer txn.Abort()
	jobs := make([]*jobdb.Job, 3)
	for i := range jobs {
		jobs[i] = testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass1).WithQueued(true).WithPools([]string{testfixtures.TestPool})
	}
	require.NoError(t, txn.Upsert(jobs))

	first, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(first), 1)
	require.InDelta(t, 0, sch.limiter.TokensAt(clk.Now()), 1e-6)
	second, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Empty(t, ScheduledJobsFromSchedulerResult(second))
	require.InDelta(t, 0, sch.limiter.TokensAt(clk.Now()), 1e-6)
	clk.Step(advance)
	for _, ex := range executors {
		ex.LastUpdateTime = protoutil.ToTimestamp(clk.Now())
	}
	third, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(third), expected, "policy-time refill")
	wantTokens := 0.0
	if expected == 0 {
		wantTokens = 0.5
	}
	require.InDelta(t, wantTokens, sch.limiter.TokensAt(clk.Now()), 1e-6)
	sctx := third.PoolResults[0].SchedulingResult.SchedulingContext
	require.Equal(t, clk.Now(), sctx.Started)
	require.Equal(t, clk.Now(), sctx.Finished)
	require.Equal(t, sctx.Started, sctx.QueueSchedulingContexts[testfixtures.TestQueue].Created)
	require.Equal(t, time.Duration(0), sctx.Finished.Sub(sctx.Started))
	require.Greater(t, sctx.ComputationDuration, time.Duration(0))
	if expected > 0 {
		require.Greater(t, sctx.TotalNewJobSchedulingTime, time.Duration(0))
	}
	require.Contains(t, sctx.ReportString(0), "Computation duration:")
	queued := 0
	for _, job := range jobs {
		stored := txn.GetById(job.Id())
		require.NotNil(t, stored)
		if stored.Queued() {
			queued++
		} else {
			require.NotNil(t, stored.LatestRun())
		}
	}
	require.Equal(t, 2-expected, queued)
	require.Len(t, txn.GetAllLeasedJobs(), 1+expected)
}

func TestFairSchedulingAlgoPolicyClockRefillsQueueLimiter(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	executors := []*schedulerobjects.Executor{makeTestExecutor("queue-clock-test", testfixtures.TestPool)}
	repo := schedulermocks.NewMockExecutorRepository(ctrl)
	repo.EXPECT().GetExecutors(gomock.Any()).Return(executors, nil).AnyTimes()
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
	queues := schedulermocks.NewMockQueueCache(ctrl)
	queues.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{testfixtures.MakeTestQueue(), testfixtures.MakeTestQueue2()}, nil).AnyTimes()
	cfg := testfixtures.TestSchedulingConfig()
	cfg.MaximumSchedulingRate, cfg.MaximumSchedulingBurst = 100, 100
	cfg.MaximumPerQueueSchedulingRate, cfg.MaximumPerQueueSchedulingBurst = 0.1, 1
	clk := clocktesting.NewFakeClock(testfixtures.BaseTime)
	sch, err := NewFairSchedulingAlgo(cfg, 0, repo, queues, reports.NewSchedulingContextRepository(), testfixtures.TestResourceListFactory, testfixtures.TestEmptyFloatingResources, priorityoverride.NewNoOpProvider(), nil, &testRunReconciler{}, WithPolicyClock(clk))
	require.NoError(t, err)
	db := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	db.SetClock(clk)
	txn := db.WriteTxn()
	defer txn.Abort()
	a := testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass1, 3)
	for i := range a {
		a[i] = a[i].WithQueued(true).WithPools([]string{testfixtures.TestPool})
	}
	require.NoError(t, txn.Upsert(a))
	first, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(first), 1)
	require.InDelta(t, 0, sch.limiterByQueue[testfixtures.TestQueue].TokensAt(clk.Now()), 1e-6)
	// Queue B has an unused token even though A has exhausted its own bucket.
	b := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue2, testfixtures.PriorityClass1).WithQueued(true).WithPools([]string{testfixtures.TestPool})
	require.NoError(t, txn.Upsert([]*jobdb.Job{b}))
	second, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(second), 1)
	require.Equal(t, b.Id(), ScheduledJobsFromSchedulerResult(second)[0].Id())
	require.InDelta(t, 0, sch.limiterByQueue[testfixtures.TestQueue].TokensAt(clk.Now()), 1e-6)
	require.InDelta(t, 0, sch.limiterByQueue[testfixtures.TestQueue2].TokensAt(clk.Now()), 1e-6)
	require.True(t, txn.GetById(a[1].Id()).Queued())
	clk.Step(20 * time.Second)
	for _, ex := range executors {
		ex.LastUpdateTime = protoutil.ToTimestamp(clk.Now())
	}
	third, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(third), 1)
	require.Equal(t, testfixtures.TestQueue, ScheduledJobsFromSchedulerResult(third)[0].Queue())
	require.InDelta(t, 0, sch.limiterByQueue[testfixtures.TestQueue].TokensAt(clk.Now()), 1e-6)
	require.True(t, txn.GetById(a[2].Id()).Queued())
	require.Greater(t, sch.limiter.TokensAt(clk.Now()), float64(1))
}

// A full node holds queue A's running work. Queue B's valid backlog can gain
// fair share only by preempting A; a nonpreemptible A job remains protected.
func TestFairSchedulingAlgoPolicyClockRefillsFairsharePreemption(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	executor := test1Node32CoreExecutor("preemption-clock-test")
	repo := schedulermocks.NewMockExecutorRepository(ctrl)
	repo.EXPECT().GetExecutors(gomock.Any()).Return([]*schedulerobjects.Executor{executor}, nil).AnyTimes()
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
	queues := schedulermocks.NewMockQueueCache(ctrl)
	queues.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{testfixtures.MakeTestQueue(), testfixtures.MakeTestQueue2()}, nil).AnyTimes()
	cfg := testfixtures.TestSchedulingConfig()
	cfg.Pools[0].FairsharePreemptionRateLimit = &configuration.RateLimit{MaximumRate: 0.1, MaximumBurst: 1}
	clk := clocktesting.NewFakeClock(testfixtures.BaseTime)
	sch, err := NewFairSchedulingAlgo(cfg, 0, repo, queues, reports.NewSchedulingContextRepository(), testfixtures.TestResourceListFactory, testfixtures.TestEmptyFloatingResources, priorityoverride.NewNoOpProvider(), nil, &testRunReconciler{}, WithPolicyClock(clk))
	require.NoError(t, err)
	db := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	db.SetClock(clk)
	txn := db.WriteTxn()
	defer txn.Abort()
	node := executor.Nodes[0]
	running := testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass0, 31)
	protected := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass2NonPreemptible)
	running = append(running, protected)
	for i, job := range running {
		job = job.WithQueued(false).WithPools([]string{testfixtures.TestPool}).WithNewRun(executor.Id, node.Id, node.Name, node.Pool, job.PriorityClass().Priority)
		running[i] = job
		node.StateByJobRunId[job.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING
	}
	backlog := testfixtures.N1Cpu4GiJobs(testfixtures.TestQueue2, testfixtures.PriorityClass0, 3)
	for i := range backlog {
		backlog[i] = backlog[i].WithQueued(true).WithPools([]string{testfixtures.TestPool})
	}
	require.NoError(t, txn.Upsert(append(running, backlog...)))

	first, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, PreemptedJobsFromSchedulerResult(first), 1)
	require.Len(t, ScheduledJobsFromSchedulerResult(first), 1)
	require.Equal(t, schedulercontext.PreemptedWithFairsharePreemption, first.PoolResults[0].SchedulingResult.PreemptedJobs[0].PreemptionType)
	require.InDelta(t, 0, sch.preemptionLimiterByPool[testfixtures.TestPool].TokensAt(clk.Now()), 1e-6)
	require.Equal(t, testfixtures.TestQueue2, ScheduledJobsFromSchedulerResult(first)[0].Queue())
	require.False(t, txn.GetById(protected.Id()).Failed())
	require.True(t, txn.GetById(backlog[1].Id()).Queued())
	// Carry the produced runs into the next independent executor snapshot.
	for _, job := range PreemptedJobsFromSchedulerResult(first) {
		delete(node.StateByJobRunId, runningRunId(t, running, job.Id()))
	}
	for _, job := range ScheduledJobsFromSchedulerResult(first) {
		node.StateByJobRunId[job.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING
	}
	second, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Empty(t, PreemptedJobsFromSchedulerResult(second))
	require.Empty(t, ScheduledJobsFromSchedulerResult(second))
	require.InDelta(t, 0, sch.preemptionLimiterByPool[testfixtures.TestPool].TokensAt(clk.Now()), 1e-6)
	clk.Step(20 * time.Second)
	executor.LastUpdateTime = protoutil.ToTimestamp(clk.Now())
	third, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, PreemptedJobsFromSchedulerResult(third), 1)
	require.Len(t, ScheduledJobsFromSchedulerResult(third), 1)
	require.Equal(t, schedulercontext.PreemptedWithFairsharePreemption, third.PoolResults[0].SchedulingResult.PreemptedJobs[0].PreemptionType)
	require.InDelta(t, 0, sch.preemptionLimiterByPool[testfixtures.TestPool].TokensAt(clk.Now()), 1e-6)
	require.False(t, txn.GetById(protected.Id()).Failed())
}

func runningRunId(t *testing.T, jobs []*jobdb.Job, id string) string {
	t.Helper()
	for _, job := range jobs {
		if job.Id() == id {
			return job.LatestRun().Id()
		}
	}
	t.Fatalf("running job %s not found", id)
	return ""
}

func TestFairSchedulingAlgoPolicyClockKeepsHostCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		makeContext func() (*armadacontext.Context, context.CancelFunc)
	}{
		{"cancelled", func() (*armadacontext.Context, context.CancelFunc) {
			ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
			cancel()
			return ctx, cancel
		}},
		{"deadline", func() (*armadacontext.Context, context.CancelFunc) {
			return armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(-time.Second))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.makeContext()
			defer cancel()
			ctrl := gomock.NewController(t)
			executors := []*schedulerobjects.Executor{makeTestExecutor("cancel-clock-test", testfixtures.TestPool)}
			repo := schedulermocks.NewMockExecutorRepository(ctrl)
			repo.EXPECT().GetExecutors(gomock.Any()).Return(executors, nil).AnyTimes()
			repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
			queues := schedulermocks.NewMockQueueCache(ctrl)
			queues.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{testfixtures.MakeTestQueue()}, nil).AnyTimes()
			clk := clocktesting.NewFakeClock(testfixtures.BaseTime)
			sch, err := NewFairSchedulingAlgo(testfixtures.TestSchedulingConfig(), time.Minute, repo, queues, reports.NewSchedulingContextRepository(), testfixtures.TestResourceListFactory, testfixtures.TestEmptyFloatingResources, priorityoverride.NewNoOpProvider(), nil, &testRunReconciler{}, WithPolicyClock(clk))
			require.NoError(t, err)
			db := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			db.SetClock(clk)
			txn := db.WriteTxn()
			defer txn.Abort()
			job := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass1).WithQueued(true).WithPools([]string{testfixtures.TestPool})
			require.NoError(t, txn.Upsert([]*jobdb.Job{job}))
			result, err := sch.Schedule(ctx, txn)
			require.NoError(t, err)
			require.Len(t, result.PoolResults, 1)
			require.Equal(t, PoolSchedulingTerminationReasonTimeout, result.PoolResults[0].Outcome.TerminationReason())
			require.Error(t, result.PoolResults[0].Outcome.Error())
			require.True(t, txn.GetById(job.Id()).Queued())
		})
	}
}

func TestFairSchedulingAlgoDefaultClockUsesHostTime(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	executors := []*schedulerobjects.Executor{makeTestExecutor("default-clock-test", testfixtures.TestPool)}
	executors[0].LastUpdateTime = protoutil.ToTimestamp(time.Now())
	repo := schedulermocks.NewMockExecutorRepository(ctrl)
	repo.EXPECT().GetExecutors(gomock.Any()).Return(executors, nil).AnyTimes()
	repo.EXPECT().GetExecutorSettings(gomock.Any()).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
	queues := schedulermocks.NewMockQueueCache(ctrl)
	queues.EXPECT().GetAll(gomock.Any()).Return([]*api.Queue{testfixtures.MakeTestQueue()}, nil).AnyTimes()
	sch, err := NewFairSchedulingAlgo(testfixtures.TestSchedulingConfig(), 0, repo, queues, reports.NewSchedulingContextRepository(), testfixtures.TestResourceListFactory, testfixtures.TestEmptyFloatingResources, priorityoverride.NewNoOpProvider(), nil, &testRunReconciler{})
	require.NoError(t, err)
	db := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := db.WriteTxn()
	defer txn.Abort()
	job := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass1).WithQueued(true).WithPools([]string{testfixtures.TestPool})
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))
	before := time.Now()
	result, err := sch.Schedule(ctx, txn)
	after := time.Now()
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(result), 1)
	sctx := result.PoolResults[0].SchedulingResult.SchedulingContext
	require.False(t, sctx.Started.Before(before))
	require.False(t, sctx.Finished.After(after))
	require.False(t, sctx.Finished.Before(sctx.Started))
	require.Equal(t, sctx.Started, sctx.QueueSchedulingContexts[testfixtures.TestQueue].Created)
	require.Greater(t, sctx.ComputationDuration, time.Duration(0))
	require.Greater(t, sctx.TotalNewJobSchedulingTime, time.Duration(0))
}
