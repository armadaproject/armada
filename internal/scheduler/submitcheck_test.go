package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestSubmitChecker_CheckJobDbJobs(t *testing.T) {
	defaultTimeout := 15 * time.Minute
	baseTime := time.Now().UTC()
	// expiredTime := baseTime.Add(-defaultTimeout).Add(-1 * time.Second)
	smallJob1 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	smallJob2 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	largeJob1 := testfixtures.Test32Cpu256GiJob("queue", testfixtures.PriorityClass1)

	// This Gang job will fit
	smallGangJob := testfixtures.
		WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("queue", testfixtures.PriorityClass1, 2))

	// This gang job doesn't fit as we only have room for three of these jobs
	largeGangJob := testfixtures.
		WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("queue", testfixtures.PriorityClass1, 4))

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		jobs           []*jobdb.Job
		expectedResult map[string]schedulingResult
	}{
		"One job schedulable": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, multiple executors": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.TestExecutor(baseTime),
				testfixtures.TestExecutor(baseTime),
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, multiple executors but only fits on one": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.TestExecutor(baseTime),
				{
					Id:             uuid.NewString(),
					Pool:           "cpu",
					LastUpdateTime: baseTime,
					Nodes:          nil,
				},
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, multiple pools": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.TestExecutor(baseTime),
				{
					Id:             uuid.NewString(),
					Pool:           "cpu2",
					LastUpdateTime: baseTime,
					Nodes:          testfixtures.TestCluster(),
				},
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu", "cpu2"}},
			},
		},
		"One job schedulable, minimum job size respected": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors: []*schedulerobjects.Executor{
				testfixtures.TestExecutor(baseTime),
				{
					Id:             uuid.NewString(),
					Pool:           "cpu2",
					LastUpdateTime: baseTime,
					MinimumJobSize: schedulerobjects.ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("100"),
						},
					},
					Nodes: testfixtures.TestCluster(),
				},
			},
			jobs: []*jobdb.Job{smallJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"Two jobs schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, smallJob2},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
				smallJob2.Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"One job schedulable, one not due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, largeJob1},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: true, pools: []string{"cpu"}},
				largeJob1.Id(): {isSchedulable: false},
			},
		},
		"No jobs schedulable due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{largeJob1},
			expectedResult: map[string]schedulingResult{
				largeJob1.Id(): {isSchedulable: false},
			},
		},
		"No jobs schedulable due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, smallJob1)},
			expectedResult: map[string]schedulingResult{
				smallJob1.Id(): {isSchedulable: false},
			},
		},
		"Gang Schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           smallGangJob,
			expectedResult: map[string]schedulingResult{
				smallGangJob[0].Id(): {isSchedulable: true, pools: []string{"cpu"}},
				smallGangJob[1].Id(): {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
		"Individual jobs fit but gang doesn't": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
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
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{largeGangJob[0], smallJob1, largeGangJob[1], largeGangJob[2], largeGangJob[3]},
			expectedResult: map[string]schedulingResult{
				largeGangJob[0].Id(): {isSchedulable: false},
				largeGangJob[1].Id(): {isSchedulable: false},
				largeGangJob[2].Id(): {isSchedulable: false},
				largeGangJob[3].Id(): {isSchedulable: false},
				smallJob1.Id():       {isSchedulable: true, pools: []string{"cpu"}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			fakeClock := clock.NewFakeClock(baseTime)
			submitCheck := NewSubmitChecker(tc.config, mockExecutorRepo, testfixtures.TestResourceListFactory)
			submitCheck.clock = fakeClock
			submitCheck.updateExecutors(ctx)
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
