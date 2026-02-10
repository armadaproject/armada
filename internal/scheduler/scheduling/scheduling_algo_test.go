package scheduling

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

type scheduledJobs struct {
	jobs         []*jobdb.Job
	acknowledged bool
}

func TestSchedule_PoolFailureIsolation(t *testing.T) {
	type poolSchedulingInfo struct {
		name               string
		recoverableError   bool
		unrecoverableError bool
	}
	tests := map[string]struct {
		pools                          []poolSchedulingInfo
		disableIndependentPoolFailures bool

		expectError               bool
		expectedSuccessfulPools   []string
		expectedUnsuccessfulPools []string
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
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			ctrl := gomock.NewController(t)

			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepoCallCount := 0
			mockExecutorRepo.EXPECT().GetExecutors(ctx).DoAndReturn(
				func(ctx *armadacontext.Context) ([]*schedulerobjects.Executor, error) {
					executorRepoCallCount++
					if tc.pools[executorRepoCallCount-1].unrecoverableError {
						return nil, fmt.Errorf("simulated critical failure for pool")
					}
					return []*schedulerobjects.Executor{}, nil
				},
			).AnyTimes()
			mockExecutorRepo.EXPECT().GetExecutorSettings(ctx).Return([]*schedulerobjects.ExecutorSettings{}, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			queueCacheCallCount := 0
			// TODO This is a hack, we should refactor so we can inject a failing scheduler and simulate scheduling failing directly
			mockQueueCache.EXPECT().GetAll(gomock.Any()).DoAndReturn(
				func(ctx *armadacontext.Context) ([]*api.Queue, error) {
					queueCacheCallCount++
					if tc.pools[queueCacheCallCount-1].recoverableError {
						return nil, fmt.Errorf("simulated recoverable failure for pool")
					}
					return []*api.Queue{}, nil
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

			sch, err := NewFairSchedulingAlgo(
				schedulingConfig,
				0,
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

			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()

			schedulerResult, err := sch.Schedule(ctx, nil, txn)
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
			AwayPools: []string{testfixtures.TestPool2},
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

		// Indices of queued jobs expected to be scheduled.
		expectedScheduledIndices []int
		// Number of jobs expected to be scheduled by pool
		expectedScheduledByPool map[string]int
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
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			mockExecutorRepo.EXPECT().GetExecutorSettings(ctx).Return(defaultExecutorSettings, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			mockQueueCache.EXPECT().GetAll(ctx).Return(tc.queues, nil).AnyTimes()

			schedulingContextRepo := reports.NewSchedulingContextRepository()
			runReconciler := &testRunReconciler{}
			sch, err := NewFairSchedulingAlgo(
				tc.schedulingConfig,
				0, // maxSchedulingDuration (disabled)
				0, // newJobsSchedulingTimeout (disabled)
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
			schedulerResult, err := sch.Schedule(ctx, nil, txn)
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

			err = populateNodeDb(nodeDb, tc.Jobs, []*jobdb.Job{}, []*internaltypes.Node{tc.Node})
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
				actualJobIds := maps.Keys(node.AllocatedByJobId)
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

				err = populateNodeDb(nodeDb, jobs, []*jobdb.Job{}, dbNodes)
				require.NoError(b, err)
			}
		})
	}
}

func TestMarkResourceUnallocatable(t *testing.T) {
	input := map[int32]internaltypes.ResourceList{
		100:  makeResourceList("cpu", "500"),
		1000: makeResourceList("cpu", "900"),
	}

	expected := map[int32]internaltypes.ResourceList{
		100:  makeResourceList("cpu", "400"),
		1000: makeResourceList("cpu", "800"),
	}

	markResourceUnallocatable(input, makeResourceList("cpu", "100"))
	assert.Equal(t, expected, input)
}

func TestMarkResourceUnallocatable_ProtectsFromNegativeValues(t *testing.T) {
	input := map[int32]internaltypes.ResourceList{
		100:  makeResourceList("cpu", "500"),
		1000: makeResourceList("cpu", "900"),
	}

	expected := map[int32]internaltypes.ResourceList{
		100:  makeResourceList("cpu", "0"),
		1000: makeResourceList("cpu", "300"),
	}

	markResourceUnallocatable(input, makeResourceList("cpu", "600"))
	assert.Equal(t, expected, input)
}

func makeResourceList(resourceName string, value string) internaltypes.ResourceList {
	return testfixtures.TestResourceListFactory.FromNodeProto(map[string]*k8sResource.Quantity{
		resourceName: pointer.MustParseResource(value),
	},
	)
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

type testRunReconciler struct {
	jobIdsToFailReconciliation []string
}

func (t *testRunReconciler) ReconcileJobRuns(txn *jobdb.Txn, _ []*schedulerobjects.Executor) []*FailedReconciliationResult {
	if t.jobIdsToFailReconciliation == nil || len(t.jobIdsToFailReconciliation) == 0 {
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
