package scheduling

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/prioritymultiplier"
	"github.com/armadaproject/armada/internal/scheduler/priorityoverride"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestSchedule(t *testing.T) {
	multiPoolSchedulingConfig := testfixtures.TestSchedulingConfig()
	multiPoolSchedulingConfig.Pools = []configuration.PoolConfig{
		{Name: testfixtures.TestPool},
		{Name: testfixtures.TestPool2},
		{
			Name:      testfixtures.AwayPool,
			AwayPools: []string{testfixtures.TestPool2},
		},
	}
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
				test1Node32CoreExecutor("executor1"),
				test1Node32CoreExecutor("executor2"),
			},
			queues:                   []*api.Queue{testfixtures.MakeTestQueue()},
			queuedJobs:               testfixtures.N16Cpu128GiJobs(testfixtures.TestQueue, testfixtures.PriorityClass3, 10),
			expectedScheduledIndices: []int{0, 1, 2, 3},
			expectedScheduledByPool:  map[string]int{testfixtures.TestPool: 4},
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
			queuedJobs:       testfixtures.N32Cpu256GiJobs("queue2", testfixtures.PriorityClass0, 1),
			scheduledJobsByExecutorIndexAndNodeIndex: map[int]map[int]scheduledJobs{
				0: {
					0: scheduledJobs{
						jobs:         testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu16GiJobs("queue1", testfixtures.PriorityClass0, 2)),
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
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			mockQueueCache.EXPECT().GetAll(ctx).Return(tc.queues, nil).AnyTimes()

			schedulingContextRepo := reports.NewSchedulingContextRepository()
			sch, err := NewFairSchedulingAlgo(
				tc.schedulingConfig,
				0,
				mockExecutorRepo,
				mockQueueCache,
				schedulingContextRepo,
				testfixtures.TestResourceListFactory,
				testfixtures.TestEmptyFloatingResources,
				prioritymultiplier.NewNoOpProvider(),
				priorityoverride.NewNoOpProvider(),
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
					jobs[j] = jobs[j].WithNewRun("executor-01", node.GetId(), node.GetName(), node.GetPool(), jobs[j].PriorityClass().Priority)
				}
			}
			armadaslices.Shuffle(jobs)
			schedulingConfig := testfixtures.TestSchedulingConfig()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				algo, err := NewFairSchedulingAlgo(
					schedulingConfig,
					time.Second*5,
					nil,
					nil,
					nil,
					testfixtures.TestResourceListFactory,
					testfixtures.TestEmptyFloatingResources,
					prioritymultiplier.NewNoOpProvider(),
					priorityoverride.NewNoOpProvider(),
				)
				require.NoError(b, err)
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

				err = algo.populateNodeDb(nodeDb, jobs, []*jobdb.Job{}, dbNodes)
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
	return testfixtures.TestResourceListFactory.FromNodeProto(map[string]k8sResource.Quantity{
		resourceName: k8sResource.MustParse(value),
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
		map[string]k8sResource.Quantity{
			"cpu":    k8sResource.MustParse("32"),
			"memory": k8sResource.MustParse("256Gi"),
		},
	)
}
