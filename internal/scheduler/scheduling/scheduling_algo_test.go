package scheduling

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/pointer"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/priorityoverride"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestConstructSchedulingContext_SetsFairsharePreemptionLimiter(t *testing.T) {
	tests := map[string]struct {
		rateLimit     *configuration.RateLimit
		expectLimiter bool
		expectedRate  rate.Limit
		expectedBurst int
	}{
		"configured": {
			rateLimit:     &configuration.RateLimit{MaximumRate: 10, MaximumBurst: 20},
			expectLimiter: true,
			expectedRate:  rate.Limit(10),
			expectedBurst: 20,
		},
		"configured with zero rate/burst": {
			rateLimit:     &configuration.RateLimit{MaximumRate: 0, MaximumBurst: 0},
			expectLimiter: true,
			expectedRate:  rate.Limit(0),
			expectedBurst: 0,
		},
		"unconfigured means no limiter": {
			rateLimit:     nil,
			expectLimiter: false,
		},
	}

	totalResources := testfixtures.TestResourceListFactory.FromNodeProto(
		map[string]*k8sResource.Quantity{"cpu": pointer.MustParseResource("1")},
	)
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			config := testfixtures.TestSchedulingConfig()
			config.Pools = []configuration.PoolConfig{{Name: "pool", FairsharePreemptionRateLimit: tc.rateLimit}}
			l := &FairSchedulingAlgo{
				schedulingConfig:        config,
				preemptionLimiterByPool: initialisePerPoolRateLimiters(config.Pools),
			}

			sctx, err := l.constructSchedulingContext("pool", totalResources, nil, nil, nil, nil, map[string]*api.Queue{})
			require.NoError(t, err)

			if !tc.expectLimiter {
				assert.Nil(t, sctx.FairsharePreemptionLimiter)
				return
			}
			require.NotNil(t, sctx.FairsharePreemptionLimiter)
			assert.Equal(t, tc.expectedRate, sctx.FairsharePreemptionLimiter.Limit())
			assert.Equal(t, tc.expectedBurst, sctx.FairsharePreemptionLimiter.Burst())
		})
	}
}

type scheduledJobs struct {
	jobs         []*jobdb.Job
	acknowledged bool
}

func TestSchedule_DisableSchedulingSkipsReconciliation(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)

	executors := []*schedulerobjects.Executor{makeTestExecutor("executor1", testfixtures.TestPool)}
	job := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass2NonPreemptible)
	job = job.WithQueued(false).WithPools([]string{testfixtures.TestPool}).WithNewRun(executors[0].Id, executors[0].Nodes[0].Id, executors[0].Nodes[0].Name, testfixtures.TestPool, job.PriorityClass().Priority)
	executors[0].Nodes[0].StateByJobRunId[job.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING

	mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
	mockExecutorRepo.EXPECT().GetExecutors(gomock.Any()).Times(0)

	mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)

	schedulingConfig := testfixtures.WithReconcilerEnabled(testfixtures.TestSchedulingConfig())
	schedulingConfig.DisableScheduling = true
	sch, err := NewFairSchedulingAlgo(
		schedulingConfig,
		0,
		mockExecutorRepo,
		mockQueueCache,
		reports.NewSchedulingContextRepository(),
		testfixtures.TestResourceListFactory,
		testfixtures.TestEmptyFloatingResources,
		priorityoverride.NewNoOpProvider(),
		nil,
		&testRunReconciler{jobIdsToFailReconciliation: []string{job.Id()}},
	)
	require.NoError(t, err)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*jobdb.Job{job}))

	schedulerResult, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, schedulerResult.PoolResults, 1)
	require.Equal(t, PoolSchedulingTerminationReasonSchedulingDisabled, schedulerResult.PoolResults[0].Outcome.TerminationReason())
	require.False(t, txn.GetById(job.Id()).Failed())
}

func TestSchedule_QueuedJobWithOnlyQueuedPriorityClassSchedules(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)

	executors := []*schedulerobjects.Executor{makeTestExecutor("executor1", testfixtures.TestPool)}

	queuedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass1).
		WithQueued(true).
		WithPools([]string{testfixtures.TestPool})

	mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
	mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(executors, nil).AnyTimes()
	mockExecutorRepo.EXPECT().GetExecutorSettings(ctx).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()

	mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
	mockQueueCache.EXPECT().GetAll(ctx).Return([]*api.Queue{testfixtures.MakeTestQueue()}, nil).AnyTimes()

	schedulingConfig := testfixtures.TestSchedulingConfig()
	sch, err := NewFairSchedulingAlgo(
		schedulingConfig,
		0,
		mockExecutorRepo,
		mockQueueCache,
		reports.NewSchedulingContextRepository(),
		testfixtures.TestResourceListFactory,
		testfixtures.TestEmptyFloatingResources,
		priorityoverride.NewNoOpProvider(),
		nil,
		&testRunReconciler{},
	)
	require.NoError(t, err)
	sch.clock = clock.NewFakeClock(testfixtures.BaseTime)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*jobdb.Job{queuedJob}))

	schedulerResult, err := sch.Schedule(ctx, txn)
	require.NoError(t, err)
	require.Len(t, ScheduledJobsFromSchedulerResult(schedulerResult), 1)
}

func TestSchedule_PoolFailureIsolation(t *testing.T) {
	type poolSchedulingInfo struct {
		name                            string
		recoverableError                bool
		unrecoverableError              bool
		runningJobFailingReconciliation bool
	}
	tests := map[string]struct {
		pools                          []poolSchedulingInfo
		disableIndependentPoolFailures bool
		enableReconciler               bool

		expectError                             bool
		expectedSuccessfulPools                 []string
		expectedUnsuccessfulPools               []string
		expectedReconcileFailedJobsByPool       map[string]int
		expectFailedReconcileJobsMarkedAsFailed bool
	}{
		"one pool recoverable error - independent pool failure enabled": {
			pools:                     []poolSchedulingInfo{{name: "pool1"}, {name: "pool2", recoverableError: true}, {name: "pool3"}},
			expectedSuccessfulPools:   []string{"pool1", "pool3"},
			expectedUnsuccessfulPools: []string{"pool2"},
		},
		"one pool recoverable error - independent pool failure disabled": {
			pools:                          []poolSchedulingInfo{{name: "pool1"}, {name: "pool2", recoverableError: true}, {name: "pool3"}},
			disableIndependentPoolFailures: true,
			expectError:                    true,
		},
		"one pool unrecoverable error - independent pool failure enabled": {
			pools:       []poolSchedulingInfo{{name: "pool1"}, {name: "pool2", unrecoverableError: true}, {name: "pool3"}},
			expectError: true,
		},
		"one pool unrecoverable error - independent pool failure disabled": {
			pools:                          []poolSchedulingInfo{{name: "pool1"}, {name: "pool2", unrecoverableError: true}, {name: "pool3"}},
			disableIndependentPoolFailures: true,
			expectError:                    true,
		},
		"reconciliation result preserved when pool scheduling fails": {
			pools: []poolSchedulingInfo{
				{name: "pool1", recoverableError: true, runningJobFailingReconciliation: true},
				{name: "pool2"},
			},
			enableReconciler:                        true,
			expectedSuccessfulPools:                 []string{"pool2"},
			expectedUnsuccessfulPools:               []string{"pool1"},
			expectedReconcileFailedJobsByPool:       map[string]int{"pool1": 1},
			expectFailedReconcileJobsMarkedAsFailed: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			ctrl := gomock.NewController(t)

			executors := []*schedulerobjects.Executor{}
			runningJobsByPool := map[string]*jobdb.Job{}
			jobIdsToFailReconciliation := []string{}
			for i, p := range tc.pools {
				if !p.runningJobFailingReconciliation {
					continue
				}
				executor := makeTestExecutor(fmt.Sprintf("executor-%d", i), p.name)
				job := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.PriorityClass2NonPreemptible)
				job = job.WithQueued(false).
					WithPools([]string{p.name}).
					WithNewRun(executor.Id, executor.Nodes[0].Id, executor.Nodes[0].Name, p.name, job.PriorityClass().Priority)
				executor.Nodes[0].StateByJobRunId[job.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING
				executors = append(executors, executor)
				runningJobsByPool[p.name] = job
				jobIdsToFailReconciliation = append(jobIdsToFailReconciliation, job.Id())
			}

			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			anyUnrecoverable := false
			for _, p := range tc.pools {
				if p.unrecoverableError {
					anyUnrecoverable = true
					break
				}
			}
			mockExecutorRepo.EXPECT().GetExecutors(gomock.AssignableToTypeOf(ctx)).DoAndReturn(
				func(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
					if anyUnrecoverable {
						return nil, fmt.Errorf("simulated critical failure for pool")
					}
					return executors, nil
				},
			).AnyTimes()
			mockExecutorRepo.EXPECT().GetExecutorSettings(gomock.AssignableToTypeOf(ctx)).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			queueCacheCallCount := 0
			// TODO This is a hack, we should refactor so we can inject a failing scheduler and simulate scheduling failing directly
			mockQueueCache.EXPECT().GetAll(gomock.Any()).DoAndReturn(
				func(ctx *armadacontext.Context) ([]*api.Queue, error) {
					queueCacheCallCount++
					if tc.pools[queueCacheCallCount-1].recoverableError {
						return nil, fmt.Errorf("simulated recoverable failure for pool")
					}
					return []*api.Queue{testfixtures.MakeTestQueue()}, nil
				},
			).AnyTimes()

			pools := []configuration.PoolConfig{}
			for _, poolInfo := range tc.pools {
				pools = append(pools, configuration.PoolConfig{Name: poolInfo.name})
			}

			schedulingConfig := testfixtures.TestSchedulingConfigWithPools(pools)
			if tc.disableIndependentPoolFailures {
				schedulingConfig = testfixtures.WithIndependentPoolFailureDisabled(schedulingConfig)
			}
			if tc.enableReconciler {
				schedulingConfig = testfixtures.WithReconcilerEnabled(schedulingConfig)
			}

			sch, err := NewFairSchedulingAlgo(
				schedulingConfig,
				0,
				mockExecutorRepo,
				mockQueueCache,
				reports.NewSchedulingContextRepository(),
				testfixtures.TestResourceListFactory,
				testfixtures.TestEmptyFloatingResources,
				priorityoverride.NewNoOpProvider(),
				nil,
				&testRunReconciler{jobIdsToFailReconciliation: jobIdsToFailReconciliation},
			)
			require.NoError(t, err)

			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			for _, job := range runningJobsByPool {
				require.NoError(t, txn.Upsert([]*jobdb.Job{job}))
			}

			schedulerResult, err := sch.Schedule(ctx, txn)
			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, schedulerResult)
			} else {
				assert.NoError(t, err)
				assert.Len(t, schedulerResult.PoolResults, len(tc.expectedUnsuccessfulPools)+len(tc.expectedSuccessfulPools))
				for _, successfulPool := range tc.expectedSuccessfulPools {
					found := false
					for _, poolResult := range schedulerResult.PoolResults {
						if poolResult.Name == successfulPool {
							assert.True(t, poolResult.Outcome.Success())
							assert.NotNil(t, poolResult.ReconciliationResult)
							assert.NotNil(t, poolResult.SchedulingResult)
							found = true
							break
						}
					}
					assert.True(t, found)
				}
				for _, failedPool := range tc.expectedUnsuccessfulPools {
					found := false
					for _, poolResult := range schedulerResult.PoolResults {
						if poolResult.Name == failedPool {
							assert.False(t, poolResult.Outcome.Success())
							assert.Nil(t, poolResult.SchedulingResult)
							assert.NotNil(t, poolResult.ReconciliationResult)
							assert.Len(t, poolResult.ReconciliationResult.FailedJobs, tc.expectedReconcileFailedJobsByPool[failedPool])
							if job, ok := runningJobsByPool[failedPool]; ok && tc.expectFailedReconcileJobsMarkedAsFailed {
								assert.Equal(t, job.Id(), poolResult.ReconciliationResult.FailedJobs[0].Job.Id())
								assert.True(t, txn.GetById(job.Id()).Failed())
							}
							found = true
							break
						}
					}
					assert.True(t, found)
				}
			}
		})
	}
}

func TestSchedule(t *testing.T) {
	multiPoolSchedulingConfig := testfixtures.TestSchedulingConfig()
	defaultExecutorSettings := []*schedulerobjects.ExecutorSettings{}
	multiPoolSchedulingConfig.Pools = []configuration.PoolConfig{
		{Name: testfixtures.TestPool},
		{Name: testfixtures.TestPool2},
		{
			Name:      testfixtures.AwayPool,
			AwayPools: []configuration.AwayPoolConfig{{Name: testfixtures.TestPool2}},
		},
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

		// Indices of existing jobs the reconciler will fail to reconcile.
		// Uses the same structure as scheduledJobsByExecutorIndexAndNodeIndex.
		jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex map[int]map[int][]int

		// Indices of existing jobs expected to be preempted.
		// Uses the same structure as scheduledJobsByExecutorIndexAndNodeIndex.
		expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex map[int]map[int][]int

		// Indices of existing jobs expected to be preempted due to reconciliation.
		// Uses the same structure as scheduledJobsByExecutorIndexAndNodeIndex.
		expectedPreemptedDueToReconciliationByExecutorIndexAndNodeIndex map[int]map[int][]int

		// Indices of existing jobs expected to be failed due to reconciliation.
		// Uses the same structure as scheduledJobsByExecutorIndexAndNodeIndex.
		expectedFailedDueToReconciliationByExecutorIndexAndNodeIndex map[int]map[int][]int

		// If true, verify that jobs preempted because of another gang member's
		// reconciliation failure include that member's failure reason.
		expectGangReconciliationReason bool

		// Indices of queued jobs expected to be scheduled.
		expectedScheduledIndices []int
		// Number of jobs expected to be scheduled by pool
		expectedScheduledByPool map[string]int
		// If set, at least one scheduled job is expected to have been placed using this scheduling method.
		expectedSchedulingMethod schedulercontext.SchedulingType
	}{
		"scheduling": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1, 2, 3},
			expectedScheduledByPool:  map[string]int{testfixtures.TestPool: 4},
		},
		"scheduling - disallowed job resources": {
			schedulingConfig: testfixtures.WithUnscheduledResources(testfixtures.TestSchedulingConfig(), []string{"cpu"}),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{},
			expectedScheduledByPool:  map[string]int{},
		},
		"scheduling - home scheduling disabled": {
			schedulingConfig: testfixtures.WithHomeSchedulingDisabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{},
			expectedScheduledByPool:  map[string]int{},
		},
		"scheduling - home away": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				makeTestExecutorWithNodes("executor-1",
					withLargeNodeTaint(testNodeWithPool(testfixtures.TestPool))),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithPools(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass4PreemptibleAway, 10), []string{testfixtures.TestPool}),
			expectedScheduledIndices: []int{0, 1},
			expectedScheduledByPool:  map[string]int{testfixtures.TestPool: 2},
		},
		"scheduling - home away - away scheduling disabled": {
			schedulingConfig: testfixtures.WithAwaySchedulingDisabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				makeTestExecutorWithNodes("executor-1",
					withLargeNodeTaint(testNodeWithPool(testfixtures.TestPool))),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithPools(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass4PreemptibleAway, 10), []string{testfixtures.TestPool}),
			expectedScheduledIndices: []int{},
			expectedScheduledByPool:  map[string]int{},
		},
		"scheduling - cross pool - home away": {
			schedulingConfig: multiPoolSchedulingConfig,
			executors: []*schedulerobjects.Executor{
				makeTestExecutorWithNodes("executor-1",
					withLargeNodeTaint(testNodeWithPool(testfixtures.TestPool2))),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithPools(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass4PreemptibleAway, 10), []string{testfixtures.TestPool, testfixtures.AwayPool}),
			expectedScheduledIndices: []int{0, 1},
			expectedScheduledByPool:  map[string]int{testfixtures.AwayPool: 2},
		},
		"scheduling - mixed pool clusters": {
			schedulingConfig: testfixtures.TestSchedulingConfigWithPools([]configuration.PoolConfig{{Name: "pool-1"}, {Name: "pool-2"}}),
			executors: []*schedulerobjects.Executor{
				makeTestExecutor("executor-1", "pool-1", "pool-2"),
				makeTestExecutor("executor-2", "pool-1"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithPools(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10), []string{"pool-1", "pool-2"}),
			expectedScheduledIndices: []int{0, 1, 2, 3, 4, 5},
			expectedScheduledByPool:  map[string]int{"pool-1": 4, "pool-2": 2},
		},
		"Fair share": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
				test1Node32CoreExecutor("executor1"),
				withLastUpdateTimeExecutor(testfixtures.BaseTime.Add(-1*time.Hour), test1Node32CoreExecutor("executor2")),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1},
		},
		"schedule onto executors with some unacknowledged jobs": {
			schedulingConfig: testfixtures.WithMaxUnacknowledgedJobsPerExecutorConfig(16, testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
		"reconcile - reconciliation disabled - does nothing": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass6Preemptible, 2),
						acknowledged: true,
					},
				},
			},
			jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
		},
		"reconcile - preemptible job - preempted": {
			schedulingConfig: testfixtures.WithReconcilerEnabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass6Preemptible, 2),
						acknowledged: true,
					},
				},
			},
			jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedPreemptedDueToReconciliationByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
		},
		"reconcile - non-preemptible job - failed": {
			schedulingConfig: testfixtures.WithReconcilerEnabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass2NonPreemptible, 2),
						acknowledged: true,
					},
				},
			},
			jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedFailedDueToReconciliationByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
		},
		"reconcile - gang job - preempts all members": {
			schedulingConfig: testfixtures.WithReconcilerEnabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("queue1", testfixtures.PriorityClass6Preemptible, 2)),
						acknowledged: true,
					},
				},
			},
			jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedPreemptedDueToReconciliationByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0, 1},
				},
			},
			expectGangReconciliationReason: true,
		},
		"reconcile - fills gap of preempted": {
			schedulingConfig: testfixtures.WithReconcilerEnabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			queues:     []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs: testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass6Preemptible, 2),
						acknowledged: true,
					},
				},
			},
			jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedPreemptedDueToReconciliationByExecutorIndexAndNodeIndex: map[int]map[int][]int{
				0: {
					0: {0},
				},
			},
			expectedScheduledIndices: []int{0},
		},
		"MaximumResourceFractionPerQueue hit before scheduling": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass3: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue()},
		},
		"no executors": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors:        []*schedulerobjects.Executor{},
			queues:           []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:       testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
		},
		"reschedules all jobs onto overallocated node": {
			schedulingConfig: testfixtures.WithProtectedFractionOfFairShareConfig(0, testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
			},
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.N1Cpu16GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass6Preemptible, 33),
						acknowledged: false,
					},
				},
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			expectedScheduledIndices: []int{},
			expectedScheduledByPool:  map[string]int{},
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{},
		},
		"computation of allocated resources does not confuse priority class with per-queue priority": {
			schedulingConfig: testfixtures.WithPerPriorityLimitsConfig(
				map[string]map[string]float64{
					testfixtures.PriorityClass3: {"cpu": 0.5},
				},
				testfixtures.TestSchedulingConfig(),
			),
			executors: []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
		"fair-share preemption still applies when only urgency-based preemption disabled": {
			schedulingConfig: testfixtures.WithPreemptionDisabled(false, true, testfixtures.TestSchedulingConfig()),
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			expectedSchedulingMethod: schedulercontext.ScheduledWithFairSharePreemption,
		},
		"urgency-based preemption still applies when only fair-share preemption disabled": {
			schedulingConfig: testfixtures.WithPreemptionDisabled(true, false, testfixtures.TestSchedulingConfig()),
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			expectedSchedulingMethod: schedulercontext.ScheduledWithUrgencyBasedPreemption,
		},
		"no preemption when both strategies disabled": {
			schedulingConfig: testfixtures.WithPreemptionDisabled(true, true, testfixtures.TestSchedulingConfig()),
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
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
			expectedPreemptedJobIndicesByExecutorIndexAndNodeIndex: map[int]map[int][]int{},
			expectedScheduledIndices:                               []int{},
		},
		"gang scheduling successful": {
			schedulingConfig:         testfixtures.TestSchedulingConfig(),
			executors:                []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 2)),
			expectedScheduledIndices: []int{0, 1},
		},
		"gang scheduling successful - away scheduling": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				makeTestExecutorWithNodes("executor-1",
					withLargeNodeTaint(testNodeWithPool(testfixtures.TestPool))),
			},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass4PreemptibleAway, 2)),
			expectedScheduledIndices: []int{0, 1},
		},
		"not scheduling gang away - gang away scheduling disabled": {
			schedulingConfig: testfixtures.WithGangAwaySchedulingDisabled(testfixtures.TestSchedulingConfig()),
			executors: []*schedulerobjects.Executor{
				makeTestExecutorWithNodes("executor-1",
					withLargeNodeTaint(testNodeWithPool(testfixtures.TestPool))),
			},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 2)),
			expectedScheduledIndices: []int{},
		},
		"gang scheduling successful - mixed pool clusters": {
			schedulingConfig: testfixtures.TestSchedulingConfigWithPools([]configuration.PoolConfig{{Name: "pool-1"}, {Name: "pool-2"}}),
			executors: []*schedulerobjects.Executor{
				makeTestExecutor("executor1", "pool-1", "pool-2"),
				makeTestExecutor("executor2", "pool-1"),
			},
			queues:                   []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:               testfixtures.WithPools(testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.PoolNameLabel), []string{"pool-1"}),
			expectedScheduledIndices: []int{0, 1, 2},
			expectedScheduledByPool:  map[string]int{"pool-1": 3},
		},
		"not scheduling a gang that does not fit on any executor": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:     []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs: testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.ClusterNameLabel),
		},
		"not scheduling a gang that does not fit on any pool": {
			schedulingConfig: testfixtures.TestSchedulingConfigWithPools([]configuration.PoolConfig{{Name: "pool-1"}, {Name: "pool-2"}}),
			executors:        []*schedulerobjects.Executor{makeTestExecutor("executor1", "pool-1", "pool-2")},
			queues:           []*api.Queue{{Name: "A", PriorityFactor: 0.01}},
			queuedJobs:       testfixtures.WithPools(testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass0, 3), testfixtures.ClusterNameLabel), []string{"pool-1", "pool-2"}),
		},
		"urgency-based gang preemption": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
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
			executors:        []*schedulerobjects.Executor{test1Node32CoreExecutor("executor1")},
			queues:           []*api.Queue{{Name: "queue1", PriorityFactor: 0.01}, {Name: "queue2", PriorityFactor: 0.01}},
			queuedJobs:       testfixtures.N16Cpu128GiJobs("queue2", testfixtures.PriorityClass0, 1),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						// Gang fills the node, putting the queue over fairshare
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.WithGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass0, 4)),
			expectedScheduledIndices: []int{0, 1, 2, 3},
		},
		"scheduling from paused queue": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueueCordoned()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{},
		},
		"multi-queue scheduling": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueue(), testfixtures.MakeTestQueue2()},
			queuedJobs: append(
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue2, testfixtures.PriorityClass3, 2)...,
			),
			expectedScheduledIndices: []int{0, 1, 10, 11},
		},
		"multi-queue scheduling with paused and non-paused queue": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueueCordoned(), testfixtures.MakeTestQueue()},
			queuedJobs: append(
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue1, testfixtures.PriorityClass3, 10)...,
			),
			expectedScheduledIndices: []int{0, 1, 2, 3},
		},
		"multi-queue scheduling with paused and non-paused queue large": {
			schedulingConfig: testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
				test1Node32CoreExecutor("executor3"),
				test1Node32CoreExecutor("executor4"),
			},
			queues: []*api.Queue{testfixtures.MakeTestQueueCordoned(), testfixtures.MakeTestQueue()},
			queuedJobs: append(
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
				testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue1, testfixtures.PriorityClass3, 10)...,
			),
			expectedScheduledIndices: []int{0, 1, 2, 3, 4, 5, 6, 7},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(gomock.AssignableToTypeOf(ctx)).Return(tc.executors, nil).AnyTimes()
			mockExecutorRepo.EXPECT().GetExecutorSettings(gomock.AssignableToTypeOf(ctx)).Return(defaultExecutorSettings, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			mockQueueCache.EXPECT().GetAll(gomock.AssignableToTypeOf(ctx)).Return(tc.queues, nil).AnyTimes()

			schedulingContextRepo := reports.NewSchedulingContextRepository()
			runReconciler := &testRunReconciler{}
			sch, err := NewFairSchedulingAlgo(
				tc.schedulingConfig,
				0, // maxSchedulingDuration (disabled)
				mockExecutorRepo,
				mockQueueCache,
				schedulingContextRepo,
				testfixtures.TestResourceListFactory,
				testfixtures.TestEmptyFloatingResources,
				priorityoverride.NewNoOpProvider(),
				nil,
				runReconciler,
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
						job = job.WithQueued(false).WithNewRun(executor.Id, node.Id, node.Name, node.Pool, job.PriorityClass().Priority)
						if existingJobs.acknowledged {
							run := job.LatestRun()
							node.StateByJobRunId[run.Id()] = schedulerobjects.JobRunState_RUNNING
						}
						jobsToUpsert = append(jobsToUpsert, job)
						executorIndexByJobId[job.Id()] = executorIndex
						executorNodeIndexByJobId[job.Id()] = nodeIndex
						jobIndexByJobId[job.Id()] = jobIndex
					}
				}
			}

			jobIdsToFailReconciliation := getJobIdsOfScheduledJobsByExecutorAndNodeIndex(t, tc.scheduledJobsByExecutorIndexAndNodeIndex, tc.jobsToFailReconciliationJobsByExecutorIndexAndNodeIndex)
			runReconciler.jobIdsToFailReconciliation = jobIdsToFailReconciliation

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

			expectedJobIdsFailedDueToReconciliation := getJobIdsOfScheduledJobsByExecutorAndNodeIndex(t, tc.scheduledJobsByExecutorIndexAndNodeIndex, tc.expectedFailedDueToReconciliationByExecutorIndexAndNodeIndex)
			actualJobIdsFailedDueToReconciliation := jobIdsFromReconciliationResults(schedulerResult.GetCombinedReconciliationResult().FailedJobs)
			slices.Sort(expectedJobIdsFailedDueToReconciliation)
			slices.Sort(actualJobIdsFailedDueToReconciliation)

			assert.Equal(t, expectedJobIdsFailedDueToReconciliation, actualJobIdsFailedDueToReconciliation)

			expectedJobIdsPreemptedDueToReconciliation := getJobIdsOfScheduledJobsByExecutorAndNodeIndex(t, tc.scheduledJobsByExecutorIndexAndNodeIndex, tc.expectedPreemptedDueToReconciliationByExecutorIndexAndNodeIndex)
			actualJobIdsPreemptedDueToReconciliation := jobIdsFromReconciliationResults(schedulerResult.GetCombinedReconciliationResult().PreemptedJobs)
			slices.Sort(expectedJobIdsPreemptedDueToReconciliation)
			slices.Sort(actualJobIdsPreemptedDueToReconciliation)

			assert.Equal(t, expectedJobIdsPreemptedDueToReconciliation, actualJobIdsPreemptedDueToReconciliation)

			if tc.expectGangReconciliationReason {
				expectedReason := fmt.Sprintf("other jobs in the gang failed reconciliation (%s: reconciling this run with the node failed)", jobIdsToFailReconciliation[0])
				for _, result := range schedulerResult.GetCombinedReconciliationResult().PreemptedJobs {
					if result.Job.Id() != jobIdsToFailReconciliation[0] {
						assert.Equal(t, expectedReason, result.Reason)
					}
				}
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
				for _, job := range scheduledJobs {
					index := queueIndexByJobId[job.Id()]
					// This is to check scheduling hasn't updated the original jobs scheduling details
					// Ideally we'd be even stricter here and check it has only modified expected fields (i.e added a run, incremented queue version)
					assert.Equal(t, job.SchedulingKey(), tc.queuedJobs[index].SchedulingKey())
					assert.Equal(t, job.JobSchedulingInfo(), tc.queuedJobs[index].JobSchedulingInfo())
				}
			}

			scheduledJobsPerPool := armadaslices.GroupByFunc(scheduledJobs, func(j *jobdb.Job) string {
				return j.LatestRun().Pool()
			})
			for pool, expectedScheduledCount := range tc.expectedScheduledByPool {
				jobsSchedulerOnPool, present := scheduledJobsPerPool[pool]
				assert.True(t, present)
				assert.Len(t, jobsSchedulerOnPool, expectedScheduledCount)
			}

			if tc.expectedSchedulingMethod != "" {
				actualSchedulingMethods := make([]schedulercontext.SchedulingType, 0)
				for _, jctx := range schedulerResult.GetAllScheduledJobs() {
					require.NotNil(t, jctx.PodSchedulingContext)
					actualSchedulingMethods = append(actualSchedulingMethods, jctx.PodSchedulingContext.SchedulingMethod)
				}
				assert.Contains(t, actualSchedulingMethods, tc.expectedSchedulingMethod)
			}

			// Check that preempted jobs are marked as such consistently.
			for _, job := range preemptedJobs {
				dbJob := txn.GetById(job.Id())
				assert.True(t, dbJob.Failed())
				assert.False(t, dbJob.Queued())
			}

			// Check that scheduled jobs are marked as such consistently.
			for _, jctx := range schedulerResult.GetAllScheduledJobs() {
				job := jctx.Job
				dbJob := txn.GetById(job.Id())
				assert.False(t, dbJob.Failed())
				assert.False(t, dbJob.Queued())
				dbRun := dbJob.LatestRun()
				assert.False(t, dbRun.Failed())
				assert.Equal(t, jctx.PodSchedulingContext.NodeId, dbRun.NodeId())
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
			for _, schCtx := range schedulerResult.GetAllSchedulingContexts() {
				for _, qtx := range schCtx.QueueSchedulingContexts {
					assert.NotEqual(t, 0, qtx.DemandCappedAdjustedFairShare)
					assert.NotEqual(t, 0, qtx.FairShare)
				}
			}
		})
	}
}

func jobIdsFromReconciliationResults(results []*FailedReconciliationResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		ids = append(ids, result.Job.Id())
	}
	return ids
}

func getJobIdsOfScheduledJobsByExecutorAndNodeIndex(t *testing.T, scheduledJobs map[int]map[int]scheduledJobs, jobIndexes map[int]map[int][]int) []string {
	result := []string{}
	for executorIndex, nodeIndexes := range jobIndexes {
		existingJobsByNode, exists := scheduledJobs[executorIndex]
		require.True(t, exists)

		for nodeIndex, jobIndexes := range nodeIndexes {
			nodeInfo, exists := existingJobsByNode[nodeIndex]
			require.True(t, exists)
			for _, jobIndex := range jobIndexes {
				result = append(result, nodeInfo.jobs[jobIndex].Id())
			}
		}
	}
	return result
}

func TestPopulateNodeDb(t *testing.T) {
	tests := map[string]struct {
		Jobs                    []*jobdb.Job
		Node                    *internaltypes.Node
		ExpectNodeAdded         bool
		ExpectNodeUnschedulable bool
		ExpectNodeOverAllocated bool
	}{
		"empty node": {
			Jobs:            []*jobdb.Job{},
			Node:            testfixtures.Test32CpuNode(testfixtures.TestPriorities),
			ExpectNodeAdded: true,
		},
		"node with jobs": {
			Jobs:            testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 5),
			Node:            testfixtures.Test32CpuNode(testfixtures.TestPriorities),
			ExpectNodeAdded: true,
		},
		"empty cordoned node": {
			Jobs:            []*jobdb.Job{},
			Node:            testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithSchedulable(false),
			ExpectNodeAdded: false,
		},
		"cordoned node with jobs": {
			Jobs:                    testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 5),
			Node:                    testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithSchedulable(false),
			ExpectNodeAdded:         true,
			ExpectNodeUnschedulable: true,
		},
		"overallocated node": {
			Jobs:                    testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass0, 33),
			Node:                    testfixtures.Test32CpuNode(testfixtures.TestPriorities),
			ExpectNodeAdded:         true,
			ExpectNodeUnschedulable: true,
			ExpectNodeOverAllocated: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			schedulingConfig := testfixtures.TestSchedulingConfig()
			nodeDb, err := nodedb.NewNodeDb(
				schedulingConfig.PriorityClasses,
				schedulingConfig.IndexedResources,
				schedulingConfig.IndexedTaints,
				schedulingConfig.IndexedNodeLabels,
				schedulingConfig.WellKnownNodeTypes,
				testfixtures.TestResourceListFactory,
			)
			require.NoError(t, err)

			for i, job := range tc.Jobs {
				tc.Jobs[i] = tc.Jobs[i].WithNewRun("executor-01", tc.Node.GetId(), tc.Node.GetName(), tc.Node.GetPool(), job.PriorityClass().Priority)
			}

			err = populateNodeDb(*schedulingConfig.GetPoolConfig(testfixtures.TestPool), nodeDb, tc.Jobs, []*jobdb.Job{}, []*internaltypes.Node{tc.Node})
			require.NoError(t, err)

			nodes, err := nodeDb.GetNodes()
			require.NoError(t, err)

			if tc.ExpectNodeAdded {
				assert.Len(t, nodes, 1)
				node := nodes[0]
				assert.Equal(t, tc.ExpectNodeOverAllocated, node.IsOverAllocated())
				assert.Equal(t, tc.ExpectNodeUnschedulable, node.IsUnschedulable())
				expectedJobIds := armadaslices.Map(tc.Jobs, func(job *jobdb.Job) string {
					return job.Id()
				})
				slices.Sort(expectedJobIds)
				actualJobIds := node.GetRunningJobIds()
				slices.Sort(actualJobIds)
				assert.Equal(t, expectedJobIds, actualJobIds)
			} else {
				assert.Len(t, nodes, 0)
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
					jobs[j] = jobs[j].WithNewRun("executor-01", node.GetId(), node.GetName(), node.GetPool(), jobs[j].PriorityClass().Priority)
				}
			}
			armadaslices.Shuffle(jobs)
			schedulingConfig := testfixtures.TestSchedulingConfig()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StartTimer()

				nodeDb, err := nodedb.NewNodeDb(
					schedulingConfig.PriorityClasses,
					schedulingConfig.IndexedResources,
					schedulingConfig.IndexedTaints,
					schedulingConfig.IndexedNodeLabels,
					schedulingConfig.WellKnownNodeTypes,
					testfixtures.TestResourceListFactory,
				)
				require.NoError(b, err)

				dbNodes := []*internaltypes.Node{}
				for _, node := range nodes {
					dbNodes = append(dbNodes, node.DeepCopyNilKeys())
				}

				err = populateNodeDb(*schedulingConfig.GetPoolConfig(testfixtures.TestPool), nodeDb, jobs, []*jobdb.Job{}, dbNodes)
				require.NoError(b, err)
			}
		})
	}
}

func makeTestExecutorWithNodes(executorId string, nodes ...*schedulerobjects.Node) *schedulerobjects.Executor {
	for _, node := range nodes {
		node.Name = fmt.Sprintf("%s-node", executorId)
		node.Executor = executorId
	}

	return &schedulerobjects.Executor{
		Id:             executorId,
		Pool:           testfixtures.TestPool,
		Nodes:          nodes,
		LastUpdateTime: testfixtures.BasetimeProto,
	}
}

func test1Node32CoreExecutor(executorId string) *schedulerobjects.Executor {
	node := test32CpuNode(testfixtures.TestPriorities)
	node.Name = fmt.Sprintf("%s-node", executorId)
	node.Executor = executorId
	node.Labels[testfixtures.ClusterNameLabel] = executorId
	return &schedulerobjects.Executor{
		Id:             executorId,
		Pool:           testfixtures.TestPool,
		Nodes:          []*schedulerobjects.Node{node},
		LastUpdateTime: testfixtures.BasetimeProto,
	}
}

func makeTestExecutor(executorId string, nodePools ...string) *schedulerobjects.Executor {
	nodes := []*schedulerobjects.Node{}

	for _, nodePool := range nodePools {
		node := test32CpuNode(testfixtures.TestPriorities)
		node.Name = fmt.Sprintf("%s-node", executorId)
		node.Executor = executorId
		node.Pool = nodePool
		node.Labels[testfixtures.PoolNameLabel] = nodePool
		nodes = append(nodes, node)
	}

	return &schedulerobjects.Executor{
		Id:             executorId,
		Pool:           testfixtures.TestPool,
		Nodes:          nodes,
		LastUpdateTime: testfixtures.BasetimeProto,
	}
}

func withLastUpdateTimeExecutor(lastUpdateTime time.Time, executor *schedulerobjects.Executor) *schedulerobjects.Executor {
	executor.LastUpdateTime = protoutil.ToTimestamp(lastUpdateTime)
	return executor
}

func testNodeWithPool(pool string) *schedulerobjects.Node {
	node := test32CpuNode(testfixtures.TestPriorities)
	node.Pool = pool
	node.Labels[testfixtures.PoolNameLabel] = pool
	return node
}

func withLargeNodeTaint(node *schedulerobjects.Node) *schedulerobjects.Node {
	node.Taints = append(node.Taints, &v1.Taint{Key: "largeJobsOnly", Value: "true", Effect: v1.TaintEffectNoSchedule})
	return node
}

func test32CpuNode(priorities []int32) *schedulerobjects.Node {
	return testfixtures.TestSchedulerObjectsNode(
		priorities,
		map[string]*k8sResource.Quantity{
			"cpu":    pointer.MustParseResource("32"),
			"memory": pointer.MustParseResource("256Gi"),
		},
	)
}

func TestBuildInUsePriorityClasses(t *testing.T) {
	schedulingConfig := testfixtures.TestSchedulingConfig()
	sch := &FairSchedulingAlgo{schedulingConfig: schedulingConfig}

	tests := map[string]struct {
		inUse    map[string]bool
		expected []string
	}{
		"empty in-use returns only default": {
			inUse:    map[string]bool{},
			expected: []string{testfixtures.TestDefaultPriorityClass},
		},
		"subset plus default": {
			inUse:    map[string]bool{testfixtures.PriorityClass0: true, testfixtures.PriorityClass1: true},
			expected: []string{testfixtures.PriorityClass0, testfixtures.PriorityClass1, testfixtures.TestDefaultPriorityClass},
		},
		"default already in use is not duplicated": {
			inUse:    map[string]bool{testfixtures.TestDefaultPriorityClass: true},
			expected: []string{testfixtures.TestDefaultPriorityClass},
		},
		"unknown name is ignored but default kept": {
			inUse:    map[string]bool{"does-not-exist": true},
			expected: []string{testfixtures.TestDefaultPriorityClass},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := sch.buildInUsePriorityClasses(tc.inUse)
			assert.ElementsMatch(t, tc.expected, maps.Keys(result))
			for _, pcName := range tc.expected {
				assert.Equal(t, schedulingConfig.PriorityClasses[pcName], result[pcName])
			}
		})
	}
}

// TestCalculateJobSchedulingInfo_AggregateMatchesScan validates that the JobDb
// scheduling-info aggregate derives exactly the same information as the job
// scan for queued and running jobs across home and away pools.
func TestCalculateJobSchedulingInfo_AggregateMatchesScan(t *testing.T) {
	ctx := armadacontext.Background()

	queues := map[string]*api.Queue{
		"q1": {Name: "q1"},
		"q2": {Name: "q2", Cordoned: true},
		"q3": {Name: "q3"},
	}

	queuedQ1 := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithQueued(true).WithPools([]string{"pool-1", "pool-2"})
	queuedQ2Cordoned := testfixtures.Test1Cpu4GiJob("q2", testfixtures.PriorityClass1).
		WithQueued(true).WithPools([]string{"pool-1"})
	queuedQ3AwayOnly := testfixtures.Test1Cpu4GiJob("q3", testfixtures.PriorityClass2).
		WithQueued(true).WithPools([]string{"pool-3"})
	queuedUnknownQueue := testfixtures.Test1Cpu4GiJob("unknown", testfixtures.PriorityClass0).
		WithQueued(true).WithPools([]string{"pool-1"})
	leasedQ1Active := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
	leasedQ1Inactive := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithNewRun("executor-3", "node-3", "node-3", "pool-1", 0)
	leasedQ2Cordoned := testfixtures.Test1Cpu4GiJob("q2", testfixtures.PriorityClass1).
		WithNewRun("executor-1", "node-5", "node-5", "pool-1", 0)
	leasedQ2Away := testfixtures.Test1Cpu4GiJob("q2", testfixtures.PriorityClass1).
		WithNewRun("executor-2", "node-2", "node-2", "pool-2", 0)
	leasedOtherPool := testfixtures.Test1Cpu4GiJob("q3", testfixtures.PriorityClass2).
		WithNewRun("executor-2", "node-4", "node-4", "pool-3", 0)
	terminal := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).WithFailed(true)

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{
		queuedQ1, queuedQ2Cordoned, queuedQ3AwayOnly, queuedUnknownQueue,
		leasedQ1Active, leasedQ1Inactive, leasedQ2Cordoned, leasedQ2Away, leasedOtherPool, terminal,
	})
	txn := jobDb.ReadTxn()

	currentPool := "pool-1"
	awayAllocationPools := []string{"pool-2"}
	allPools := []string{"pool-1", "pool-2"}
	activeExecutorsSet := map[string]bool{"executor-1": true, "executor-2": true}

	algo := &FairSchedulingAlgo{}
	jobs := append(txn.GetAllLeasedJobs(), getQueuedJobs(txn, allPools)...)
	scanned, err := algo.calculateJobSchedulingInfo(
		ctx, activeExecutorsSet, queues, jobs, currentPool, awayAllocationPools, allPools, nil, true, true,
	)
	require.NoError(t, err)
	aggregate := aggregateJobSchedulingInfo(txn, activeExecutorsSet, queues, currentPool, awayAllocationPools, allPools, nil, true, true)

	components, diff := compareJobSchedulingInfo(scanned, aggregate)
	require.Empty(t, components, diff)
	require.Empty(t, diff)
}

// TestCalculateJobSchedulingInfo_AggregateMatchesScanWithZeroResourceJobs proves
// that a zero-resource queued job does not trigger a false mismatch once the last
// non-zero job in its queue and priority class is removed. The aggregate drops the
// zeroed bucket while the scan keeps a zero-valued one.
func TestCalculateJobSchedulingInfo_AggregateMatchesScanWithZeroResourceJobs(t *testing.T) {
	pool := "aggregate-zero-resource-pool"
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	nonZero := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithQueued(true).WithPools([]string{pool})
	zero := testfixtures.TestJobWithResources("q1", testfixtures.PriorityClass0, v1.ResourceList{}).
		WithQueued(true).WithPools([]string{pool})

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{nonZero, zero})
	writeTxn := jobDb.WriteTxn()
	require.NoError(t, writeTxn.BatchDelete([]string{nonZero.Id()}))
	writeTxn.Commit()
	txn := jobDb.ReadTxn()

	algo := &FairSchedulingAlgo{}
	jobs := getQueuedJobs(txn, []string{pool})
	scanned, err := algo.calculateJobSchedulingInfo(
		armadacontext.Background(), map[string]bool{}, queues, jobs, pool, nil, []string{pool}, nil, true, true,
	)
	require.NoError(t, err)
	aggregate := aggregateJobSchedulingInfo(txn, map[string]bool{}, queues, pool, nil, []string{pool}, nil, true, true)

	// Compare demand only: the in-use priority classes are derived from the
	// resource aggregate, which prunes zeroed buckets. A zero-resource job that
	// outlives its non-zero siblings therefore does not register as in use. This
	// only affects synthetic zero-resource jobs; real jobs always request resources.
	require.Empty(t, compareResourceListMaps(
		"demand", scanned.demandByQueueAndPriorityClass, aggregate.demandByQueueAndPriorityClass,
	))
}

// TestCompareJobSchedulingInfo proves the comparison reports the mismatching
// components and treats zero buckets as absent.
func TestCompareJobSchedulingInfo(t *testing.T) {
	oneCpu := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).AllResourceRequirements()
	twoCpu := oneCpu.Add(oneCpu)
	pc := testfixtures.PriorityClass0

	base := func() *jobSchedulingInfo {
		return &jobSchedulingInfo{
			jobsByExecutorId:                     map[string][]*jobdb.Job{},
			jobsByPool:                           map[string][]*jobdb.Job{},
			demandByQueueAndPriorityClass:        map[string]map[string]internaltypes.ResourceList{"q1": {pc: oneCpu}},
			allocatedByQueueAndPriorityClass:     map[string]map[string]internaltypes.ResourceList{},
			awayAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
			shortJobPenaltyByQueue:               map[string]internaltypes.ResourceList{},
			inUsePriorityClasses:                 map[string]bool{pc: true},
		}
	}

	t.Run("equal info matches", func(t *testing.T) {
		components, diff := compareJobSchedulingInfo(base(), base())
		require.Empty(t, components)
		require.Empty(t, diff)
	})

	t.Run("different demand reports demand component", func(t *testing.T) {
		a := base()
		b := base()
		b.demandByQueueAndPriorityClass = map[string]map[string]internaltypes.ResourceList{"q1": {pc: twoCpu}}
		components, diff := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "demand")
		require.Contains(t, diff, "queue=q1")
	})

	t.Run("zero bucket matches absent bucket", func(t *testing.T) {
		zero := oneCpu.Subtract(oneCpu)
		a := base()
		a.demandByQueueAndPriorityClass = map[string]map[string]internaltypes.ResourceList{"q1": {pc: zero}}
		b := base()
		b.demandByQueueAndPriorityClass = map[string]map[string]internaltypes.ResourceList{}
		components, _ := compareJobSchedulingInfo(a, b)
		require.Empty(t, components)
	})

	t.Run("jobs by pool reports component", func(t *testing.T) {
		job := testfixtures.Test1Cpu4GiJob("q1", pc).WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
		a := base()
		a.jobsByPool = map[string][]*jobdb.Job{"pool-1": {job}}
		b := base()
		components, diff := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "jobs_by_pool")
		require.Contains(t, diff, "pool-1")
	})

	t.Run("jobs by executor reports component", func(t *testing.T) {
		jobA := testfixtures.Test1Cpu4GiJob("q1", pc).WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
		jobB := testfixtures.Test1Cpu4GiJob("q1", pc).WithNewRun("executor-1", "node-2", "node-2", "pool-1", 0)
		a := base()
		a.jobsByExecutorId = map[string][]*jobdb.Job{"executor-1": {jobA}}
		b := base()
		b.jobsByExecutorId = map[string][]*jobdb.Job{"executor-1": {jobB}}
		components, diff := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "jobs_by_executor")
		require.Contains(t, diff, "executor-1")
	})

	t.Run("allocated reports component", func(t *testing.T) {
		a := base()
		a.allocatedByQueueAndPriorityClass = map[string]map[string]internaltypes.ResourceList{"q1": {pc: oneCpu}}
		b := base()
		components, _ := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "allocated")
	})

	t.Run("away allocated reports component", func(t *testing.T) {
		a := base()
		a.awayAllocatedByQueueAndPriorityClass = map[string]map[string]internaltypes.ResourceList{"q1": {pc: oneCpu}}
		b := base()
		components, _ := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "away_allocated")
	})

	t.Run("in use priority classes reports component", func(t *testing.T) {
		a := base()
		b := base()
		b.inUsePriorityClasses = map[string]bool{}
		components, _ := compareJobSchedulingInfo(a, b)
		require.Contains(t, components, "in_use_priority_classes")
	})
}

// TestCalculateJobSchedulingInfo_MismatchUsesScanAndRecords proves the full
// aggregate comparison path on divergence: the mismatch is recorded in the
// armada_scheduler_job_aggregate_* metrics, and the authoritative scan result is
// returned when use is disabled.
//
// Divergence is forced by passing a jobs slice containing a queued job that was
// never upserted into the JobDb, so the scan sees it but the aggregate does not.
func TestCalculateJobSchedulingInfo_MismatchUsesScanAndRecords(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-mismatch-pool"
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	queued := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithQueued(true).WithPools([]string{pool})
	phantom := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithQueued(true).WithPools([]string{pool})

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{queued})
	txn := jobDb.ReadTxn()
	algo := &FairSchedulingAlgo{queuedAggregateDemandConfig: configuration.AggregateDemandConfig{Compare: true}}

	beforeComparisons := testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool))
	beforeMismatches := testutil.ToFloat64(jobAggregateMismatches.WithLabelValues(pool))
	beforeDemandMismatches := testutil.ToFloat64(jobAggregateMismatchComponents.WithLabelValues(pool, "demand"))

	info, err := algo.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{}, queues,
		[]*jobdb.Job{queued, phantom}, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)

	// Scan wins: demand covers both jobs (2 cpu) although the aggregate only knows one.
	cpu := info.demandByQueueAndPriorityClass["q1"][testfixtures.PriorityClass0].GetByNameZeroIfMissing("cpu")
	require.Equal(t, int64(2), cpu.Value())

	require.Equal(t, beforeComparisons+1, testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
	require.Equal(t, beforeMismatches+1, testutil.ToFloat64(jobAggregateMismatches.WithLabelValues(pool)))
	require.Equal(t, beforeDemandMismatches+1, testutil.ToFloat64(jobAggregateMismatchComponents.WithLabelValues(pool, "demand")))
}

// TestCalculateJobSchedulingInfo_RunningMismatchRecordsRunningMetrics proves a
// running-jobs divergence is recorded in the running aggregate metrics, and that
// the queued metrics are left untouched.
func TestCalculateJobSchedulingInfo_RunningMismatchRecordsRunningMetrics(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-running-mismatch-pool"
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	leased := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithNewRun("executor-1", "node-1", "node-1", pool, 0)
	phantom := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).
		WithNewRun("executor-1", "node-1", "node-1", pool, 0)

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{leased})
	txn := jobDb.ReadTxn()
	algo := &FairSchedulingAlgo{runningAggregateDemandConfig: configuration.AggregateDemandConfig{Compare: true}}

	beforeComparisons := testutil.ToFloat64(jobAggregateRunningComparisons.WithLabelValues(pool))
	beforeMismatches := testutil.ToFloat64(jobAggregateRunningMismatches.WithLabelValues(pool))
	beforeDemandMismatches := testutil.ToFloat64(jobAggregateRunningMismatchComponents.WithLabelValues(pool, "demand"))
	beforeQueuedComparisons := testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool))

	_, err := algo.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{"executor-1": true}, queues,
		[]*jobdb.Job{leased, phantom}, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)

	require.Equal(t, beforeComparisons+1, testutil.ToFloat64(jobAggregateRunningComparisons.WithLabelValues(pool)))
	require.Equal(t, beforeMismatches+1, testutil.ToFloat64(jobAggregateRunningMismatches.WithLabelValues(pool)))
	require.Equal(t, beforeDemandMismatches+1, testutil.ToFloat64(jobAggregateRunningMismatchComponents.WithLabelValues(pool, "demand")))
	require.Equal(t, beforeQueuedComparisons, testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
}

// TestCalculateJobSchedulingInfo_AggregateDisabledPublishesNothing proves the
// default (flags off) path neither computes nor publishes anything.
func TestCalculateJobSchedulingInfo_AggregateDisabledPublishesNothing(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-demand-disabled-pool"
	queued := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).WithQueued(true).WithPools([]string{pool})
	phantom := testfixtures.Test1Cpu4GiJob("q1", testfixtures.PriorityClass0).WithQueued(true).WithPools([]string{pool})
	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{queued})
	txn := jobDb.ReadTxn()
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}
	algo := &FairSchedulingAlgo{}

	before := testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool))
	_, err := algo.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{}, queues,
		[]*jobdb.Job{queued, phantom}, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, before, testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
}

// TestCalculateJobSchedulingInfo_AggregateAgreementPublishesComparisonOnly proves
// that when the aggregate agrees with the scan the comparison records a comparison
// but no mismatch, and the scan-derived info is used.
func TestCalculateJobSchedulingInfo_AggregateAgreementPublishesComparisonOnly(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-demand-agreement-pool"
	pc := testfixtures.PriorityClass0
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	queued := testfixtures.Test1Cpu4GiJob("q1", pc).WithQueued(true).WithPools([]string{pool})
	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{queued})
	txn := jobDb.ReadTxn()
	algo := &FairSchedulingAlgo{queuedAggregateDemandConfig: configuration.AggregateDemandConfig{Compare: true}}

	beforeComparisons := testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool))
	beforeMismatches := testutil.ToFloat64(jobAggregateMismatches.WithLabelValues(pool))

	info, err := algo.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{}, queues,
		[]*jobdb.Job{queued}, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)

	require.Equal(t, beforeComparisons+1, testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
	require.Equal(t, beforeMismatches, testutil.ToFloat64(jobAggregateMismatches.WithLabelValues(pool)))

	cpu := info.demandByQueueAndPriorityClass["q1"][pc].GetByNameZeroIfMissing("cpu")
	require.Equal(t, int64(1), cpu.Value())
}

// TestCalculateJobSchedulingInfo_UsesAggregateAndSkipsScan proves that with use
// enabled (and compare disabled) the scheduling info is sourced from the
// aggregate and the passed jobs are ignored, so the scheduler need not fetch or
// scan any jobs. Running demand is now also sourced from the aggregate.
func TestCalculateJobSchedulingInfo_UsesAggregateAndSkipsScan(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-use-pool"
	pc := testfixtures.PriorityClass0
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	queued := testfixtures.Test1Cpu4GiJob("q1", pc).WithQueued(true).WithPools([]string{pool})
	leased := testfixtures.Test1Cpu4GiJob("q1", pc).
		WithNewRun("executor-1", "node-1", "node-1", pool, 0)
	phantomQueued := testfixtures.Test1Cpu4GiJob("q1", pc).WithQueued(true).WithPools([]string{pool})
	phantomLeased := testfixtures.Test1Cpu4GiJob("q1", pc).
		WithNewRun("executor-2", "node-2", "node-2", pool, 0)

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{queued, leased})
	txn := jobDb.ReadTxn()
	algo := &FairSchedulingAlgo{
		queuedAggregateDemandConfig:  configuration.AggregateDemandConfig{Use: true},
		runningAggregateDemandConfig: configuration.AggregateDemandConfig{Use: true},
	}

	before := testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool))

	info, err := algo.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{"executor-1": true, "executor-2": true}, queues,
		[]*jobdb.Job{phantomQueued, phantomLeased}, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)

	// Only the jobs in the JobDb contribute: queued + leased = 2 cpu. The
	// phantoms passed in the jobs slice are ignored, proving the scan was skipped.
	cpu := info.demandByQueueAndPriorityClass["q1"][pc].GetByNameZeroIfMissing("cpu")
	require.Equal(t, int64(2), cpu.Value())

	// No comparison is published when compare is off.
	require.Equal(t, before, testutil.ToFloat64(jobAggregateComparisons.WithLabelValues(pool)))
}

// TestCalculateJobSchedulingInfo_IndependentAggregateFlags proves that queued
// and running jobs can be sourced from the aggregate independently: the category
// with Use set ignores the jobs passed to the scan, while the other category is
// still derived from the scan.
func TestCalculateJobSchedulingInfo_IndependentAggregateFlags(t *testing.T) {
	ctx := armadacontext.Background()
	pool := "aggregate-independent-pool"
	pc := testfixtures.PriorityClass0
	queues := map[string]*api.Queue{"q1": {Name: "q1"}}

	queuedReal := testfixtures.Test1Cpu4GiJob("q1", pc).WithQueued(true).WithPools([]string{pool})
	leasedReal := testfixtures.Test1Cpu4GiJob("q1", pc).
		WithNewRun("executor-1", "node-1", "node-1", pool, 0)
	phantomQueued := testfixtures.TestJobWithResources("q1", pc, v1.ResourceList{
		"cpu": *k8sResource.NewQuantity(5, k8sResource.DecimalSI),
	}).WithQueued(true).WithPools([]string{pool})
	phantomLeased := testfixtures.TestJobWithResources("q1", pc, v1.ResourceList{
		"cpu": *k8sResource.NewQuantity(3, k8sResource.DecimalSI),
	}).WithNewRun("executor-2", "node-2", "node-2", pool, 0)

	jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{queuedReal, leasedReal})
	txn := jobDb.ReadTxn()
	jobs := []*jobdb.Job{phantomQueued, phantomLeased}

	// Queued from the aggregate (1) and running from the scan (3).
	queuedOnly := &FairSchedulingAlgo{queuedAggregateDemandConfig: configuration.AggregateDemandConfig{Use: true}}
	info, err := queuedOnly.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{"executor-1": true}, queues, jobs, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)
	cpu := info.demandByQueueAndPriorityClass["q1"][pc].GetByNameZeroIfMissing("cpu")
	require.Equal(t, int64(4), cpu.Value())

	// Running from the aggregate (1) and queued from the scan (5).
	runningOnly := &FairSchedulingAlgo{runningAggregateDemandConfig: configuration.AggregateDemandConfig{Use: true}}
	info, err = runningOnly.newCalculateJobSchedulingInfo(
		ctx, txn, map[string]bool{"executor-1": true}, queues, jobs, pool, nil, []string{pool}, nil,
	)
	require.NoError(t, err)
	cpu = info.demandByQueueAndPriorityClass["q1"][pc].GetByNameZeroIfMissing("cpu")
	require.Equal(t, int64(6), cpu.Value())
}

// BenchmarkSchedulingInfo is an end-to-end comparison, not a like-for-like one:
// the scan case gathers the jobs and builds the full scheduling info, while the
// aggregate case only performs the isolated aggregate lookup.
func BenchmarkSchedulingInfo(b *testing.B) {
	const (
		numQueues         = 8
		numQueuedPerQueue = 2000
		numLeasedPerQueue = 500
	)

	poolNames := []string{"pool-1", "pool-2", "pool-3", "pool-4"}
	queueNames := make([]string, numQueues)
	for i := range queueNames {
		queueNames[i] = fmt.Sprintf("queue-%d", i)
	}

	jobs := make([]*jobdb.Job, 0, numQueues*(numQueuedPerQueue+numLeasedPerQueue))
	for _, queueName := range queueNames {
		for i := 0; i < numQueuedPerQueue; i++ {
			jobs = append(jobs, testfixtures.Test1Cpu4GiJob(queueName, testfixtures.PriorityClass0).
				WithQueued(true).WithPools(poolNames))
		}
		for i := 0; i < numLeasedPerQueue; i++ {
			pool := poolNames[i%len(poolNames)]
			executor := fmt.Sprintf("executor-%d", i%len(poolNames))
			jobs = append(jobs, testfixtures.Test1Cpu4GiJob(queueName, testfixtures.PriorityClass0).
				WithNewRun(executor, fmt.Sprintf("node-%d", i), fmt.Sprintf("node-%d", i), pool, 0))
		}
	}

	jobDb := testfixtures.NewJobDbWithJobs(jobs)
	txn := jobDb.ReadTxn()

	queues := make(map[string]*api.Queue, numQueues)
	for _, queueName := range queueNames {
		queues[queueName] = &api.Queue{Name: queueName}
	}
	activeExecutorsSet := map[string]bool{}
	for i := 0; i < len(poolNames); i++ {
		activeExecutorsSet[fmt.Sprintf("executor-%d", i)] = true
	}

	currentPool := poolNames[0]
	awayAllocationPools := poolNames[1:]
	allPools := poolNames
	algo := &FairSchedulingAlgo{}
	ctx := armadacontext.Background()

	b.Run("impl=full_scheduling_info", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			allJobs := append(txn.GetAllLeasedJobs(), getQueuedJobs(txn, allPools)...)
			if _, err := algo.calculateJobSchedulingInfo(
				ctx, activeExecutorsSet, queues, allJobs, currentPool, awayAllocationPools, allPools, nil, true, true,
			); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("impl=aggregate_scheduling_info", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			aggregateJobSchedulingInfo(txn, activeExecutorsSet, queues, currentPool, awayAllocationPools, allPools, nil, true, true)
		}
	})
}

type testRunReconciler struct {
	jobIdsToFailReconciliation []string
}

func (t *testRunReconciler) ReconcileJobRuns(txn *jobdb.Txn, _ []*schedulerobjects.Executor) []*FailedReconciliationResult {
	if len(t.jobIdsToFailReconciliation) == 0 {
		return nil
	}
	jobs := txn.GetAll()
	result := make([]*FailedReconciliationResult, 0, len(jobs))
	for _, job := range jobs {
		if slices.Contains(t.jobIdsToFailReconciliation, job.Id()) {
			result = append(result, &FailedReconciliationResult{Job: job, Reason: "reconciling this run with the node failed"})
		}
	}
	return result
}
