package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestSubmitChecker_CheckJobDbJobs(t *testing.T) {
	defaultTimeout := 15 * time.Minute
	baseTime := time.Now().UTC()
	// expiredTime := baseTime.Add(-defaultTimeout).Add(-1 * time.Second)
	smallJob1 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	smallJob2 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	smallGpuJob := testfixtures.Test1GpuJob("queue", testfixtures.PriorityClass4PreemptibleAway)
	smallAwayJob := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass4PreemptibleAway)
	largeJob1 := testfixtures.Test32Cpu256GiJob("queue", testfixtures.PriorityClass1)

	// This Gang job will fit
	smallGangJob := testfixtures.
		WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("queue", testfixtures.PriorityClass1, 2))

	// This gang job doesn't fit as we only have room for three of these jobs
	largeGangJob := testfixtures.
		WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("queue", testfixtures.PriorityClass1, 4))

	schedulingConfig := testfixtures.TestSchedulingConfig()
	schedulingConfig.Pools = []configuration.PoolConfig{
		{Name: "cpu"},
		{Name: "cpu2"},
		{Name: "gpu"},
		{Name: "cpu-away", AwayPools: []string{"gpu"}},
	}

	tests := map[string]struct {
		executorTimout time.Duration
		executors      []*schedulerobjects.Executor
		jobs           []*jobdb.Job
		expectedResult map[string]schedulingResult
		queue          *api.Queue
	}{
		"One job schedulable": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, multiple executors": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(SmallNode("cpu")),
				Executor(SmallNode("cpu")),
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, multiple executors but only fits on one": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu")), Executor()},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, home jobs not assigned to away pools": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(GpuNode("gpu")),
			},
			jobs: []*jobdb.Job{smallGpuJob},
			expectedResult: map[string]schedulingResult{
				// The job can theoretically schedule:
				// - On the "gpu" pool as a home job
				// - On the "cpu-away" pool, which has "gpu" as an away pool
				// Jobs shouldn't get assigned to pools with away pools they are already home jobs on
				smallGpuJob.Id(): {isSchedulable: true, pools: []string{"gpu"}},
			},
		},
		"One job schedulable, away pools": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(SmallNode("cpu")),
				Executor(GpuNode("gpu")),
			},
			jobs: []*jobdb.Job{smallAwayJob},
			expectedResult: map[string]schedulingResult{
				smallAwayJob.Id(): {isSchedulable: true, pools: []string{"cpu", "cpu-away"}},
			},
		},
		"One job schedulable, away pools, multiple executors": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(SmallNode("cpu")),
				Executor(GpuNode("gpu")),
				Executor(GpuNode("gpu")),
			},
			jobs: []*jobdb.Job{smallAwayJob},
			expectedResult: map[string]schedulingResult{
				smallAwayJob.Id(): {isSchedulable: true, pools: []string{"cpu", "cpu-away"}},
			},
		},
		"One job schedulable, multiple pools": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(SmallNode("cpu")),
				Executor(SmallNode("cpu2")),
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu", "cpu2"}},
			},
		},
		"One job schedulable, one executor, multiple pools": {
			executorTimout: defaultTimeout,
			executors: []*schedulerobjects.Executor{
				Executor(SmallNode("cpu"), SmallNode("cpu2")),
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu", "cpu2"}},
			},
		},
		"Two jobs schedules": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{smallJob1, smallJob2},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
				smallJob2.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, one not due to resources": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{smallJob1, largeJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
				largeJob1.Id(): {isSchedulable: false},
			},
		},
		"No jobs schedulable due to resources": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{largeJob1},
			expectedResult: map[string]schedulingResult{
				largeJob1.Id(): {isSchedulable: false},
			},
		},
		"No jobs schedulable due to selector": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, smallJob1)},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: false},
			},
		},
		"Gang Schedules - one cluster one node": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           smallGangJob,
			expectedResult: map[string]schedulingResult{
				smallGangJob[0].Id(): {isSchedulable: true, pools: []string{"cpu"}},
				smallGangJob[1].Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"Gang Schedules - one cluster multiple node": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"), SmallNode("cpu"))},
			jobs:           largeGangJob,
			expectedResult: map[string]schedulingResult{
				largeGangJob[0].Id(): {isSchedulable: true, pools: []string{"cpu"}},
				largeGangJob[1].Id(): {isSchedulable: true, pools: []string{"cpu"}},
				largeGangJob[2].Id(): {isSchedulable: true, pools: []string{"cpu"}},
				largeGangJob[3].Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"Individual jobs fit but gang doesn't": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           largeGangJob,
			expectedResult: map[string]schedulingResult{
				largeGangJob[0].Id(): {isSchedulable: false},
				largeGangJob[1].Id(): {isSchedulable: false},
				largeGangJob[2].Id(): {isSchedulable: false},
				largeGangJob[3].Id(): {isSchedulable: false},
			},
		},
		"Individual jobs fit but gang doesn't on mixed pool cluster": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"), SmallNode("cpu2"))},
			jobs:           largeGangJob,
			expectedResult: map[string]schedulingResult{
				largeGangJob[0].Id(): {isSchedulable: false},
				largeGangJob[1].Id(): {isSchedulable: false},
				largeGangJob[2].Id(): {isSchedulable: false},
				largeGangJob[3].Id(): {isSchedulable: false},
			},
		},
		"One job fits, one gang doesn't, out of order": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{largeGangJob[0], smallJob1, largeGangJob[1], largeGangJob[2], largeGangJob[3]},
			expectedResult: map[string]schedulingResult{
				largeGangJob[0].Id(): {isSchedulable: false},
				largeGangJob[1].Id(): {isSchedulable: false},
				largeGangJob[2].Id(): {isSchedulable: false},
				largeGangJob[3].Id(): {isSchedulable: false},
				smallJob1.Id():       {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job exceeds queue fraction limit": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: false},
			},
			queue: &api.Queue{
				Name:           "queue",
				PriorityFactor: 100,
				ResourceLimitsByPriorityClassName: map[string]*api.PriorityClassResourceLimits{
					testfixtures.PriorityClass1: {
						MaximumResourceFraction: map[string]float64{
							"cpu": 0.0001,
						},
					},
				},
			},
		},
		"One job exceeds total floating resources": {
			executorTimout: defaultTimeout,
			executors:      []*schedulerobjects.Executor{Executor(SmallNode("cpu"))},
			jobs: testfixtures.WithRequestsJobs(
				schedulerobjects.ResourceList{
					Resources: map[string]resource.Quantity{
						"test-floating-resource": resource.MustParse("11"),
					},
				},
				[]*jobdb.Job{smallJob1}),
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: false},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			queue := tc.queue
			if queue == nil {
				queue = &api.Queue{Name: "queue"}
			}

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			mockQueueCache.EXPECT().GetAll(ctx).Return([]*api.Queue{queue}, nil).AnyTimes()
			floatingResources, _ := floatingresources.NewFloatingResourceTypes(
				[]configuration.FloatingResourceConfig{
					{
						Name: "test-floating-resource",
						Pools: []configuration.FloatingResourcePoolConfig{
							{
								Name:     "cpu",
								Quantity: resource.MustParse("10"),
							},
						},
					},
				},
				testfixtures.TestResourceListFactory)
			fakeClock := clock.NewFakeClock(baseTime)
			submitCheck := NewSubmitChecker(schedulingConfig,
				mockExecutorRepo,
				mockQueueCache,
				floatingResources,
				testfixtures.TestResourceListFactory)
			submitCheck.clock = fakeClock
			err := submitCheck.Initialise(ctx)
			assert.NoError(t, err)
			results, err := submitCheck.Check(ctx, tc.jobs)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedResult), len(results))
			for id, expected := range tc.expectedResult {
				actualResult, ok := results[id]
				require.True(t, ok)
				actualResult.reason = "" // clear reason as we don't test this

				// sort pools as we don't care about order
				slices.Sort(actualResult.pools)
				slices.Sort(expected.pools)
				assert.Equal(t, expected, actualResult)
			}
		})
	}
}

func TestSubmitChecker_Initialise(t *testing.T) {
	tests := map[string]struct {
		queueCacheErr   error
		executorRepoErr error
		expectError     bool
	}{
		"Successful initialisation": {
			expectError: false,
		},
		"error on queue cache error": {
			expectError:   true,
			queueCacheErr: fmt.Errorf("failed to get queues"),
		},
		"error on executor repo err": {
			expectError:   true,
			queueCacheErr: fmt.Errorf("failed to get executors"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			queue := &api.Queue{Name: "queue"}
			executors := []*schedulerobjects.Executor{Executor(SmallNode("cpu"))}

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			if tc.executorRepoErr != nil {
				mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(nil, tc.executorRepoErr).AnyTimes()
			} else {
				mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(executors, nil).AnyTimes()
			}

			mockQueueCache := schedulermocks.NewMockQueueCache(ctrl)
			if tc.queueCacheErr != nil {
				mockQueueCache.EXPECT().GetAll(ctx).Return(nil, tc.queueCacheErr).AnyTimes()
			} else {
				mockQueueCache.EXPECT().GetAll(ctx).Return([]*api.Queue{queue}, nil).AnyTimes()
			}

			submitCheck := NewSubmitChecker(testfixtures.TestSchedulingConfig(),
				mockExecutorRepo,
				mockQueueCache,
				testfixtures.TestFloatingResources,
				testfixtures.TestResourceListFactory)

			err := submitCheck.Initialise(ctx)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Executor(nodes ...*schedulerobjects.Node) *schedulerobjects.Executor {
	executorId := uuid.NewString()
	for _, node := range nodes {
		node.Executor = executorId
	}
	return &schedulerobjects.Executor{
		Id:    executorId,
		Pool:  "cpu",
		Nodes: nodes,
	}
}

func GpuNode(pool string) *schedulerobjects.Node {
	node := testfixtures.TestSchedulerObjectsNode(
		testfixtures.TestPriorities,
		map[string]resource.Quantity{
			"cpu":            resource.MustParse("30"),
			"memory":         resource.MustParse("512Gi"),
			"nvidia.com/gpu": resource.MustParse("8"),
		})
	node.Taints = []v1.Taint{
		{
			Key:    "gpu",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	node.Pool = pool
	return node
}

func SmallNode(pool string) *schedulerobjects.Node {
	node := testfixtures.TestSchedulerObjectsNode(
		testfixtures.TestPriorities,
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("2"),
			"memory": resource.MustParse("64Gi"),
		})
	node.Pool = pool
	return node
}
