package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

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

	// This gang job doesn't fit  as we only have room for three of these joobs
	largeGangJob := testfixtures.
		WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("queue", testfixtures.PriorityClass1, 4))

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		jobs           []*jobdb.Job
		expectedResult map[string]bool
	}{
		"One job schedulable": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]bool{smallJob1.Id(): true},
		},
		"Two jobs schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, smallJob2},
			expectedResult: map[string]bool{smallJob1.Id(): true, smallJob2.Id(): true},
		},
		"One job schedulable, one not due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, largeJob1},
			expectedResult: map[string]bool{smallJob1.Id(): true, largeJob1.Id(): false},
		},
		"No jobs schedulable due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{largeJob1},
			expectedResult: map[string]bool{largeJob1.Id(): false},
		},
		"No jobs schedulable due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, smallJob1)},
			expectedResult: map[string]bool{smallJob1.Id(): false},
		},
		"Gang Schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           smallGangJob,
			expectedResult: map[string]bool{smallGangJob[0].Id(): true, smallGangJob[1].Id(): true},
		},
		"Individual jobs fit but gang doesn't": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           largeGangJob,
			expectedResult: map[string]bool{
				largeGangJob[0].Id(): false,
				largeGangJob[1].Id(): false,
				largeGangJob[2].Id(): false,
				largeGangJob[3].Id(): false,
			},
		},
		"One job fits, one gang doesn't, out of order": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{largeGangJob[0], smallJob1, largeGangJob[1], largeGangJob[2], largeGangJob[3]},
			expectedResult: map[string]bool{
				largeGangJob[0].Id(): false,
				largeGangJob[1].Id(): false,
				largeGangJob[2].Id(): false,
				largeGangJob[3].Id(): false,
				smallJob1.Id():       true,
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
			for id, expectedSchedulable := range tc.expectedResult {
				actualResult, ok := results[id]
				require.True(t, ok)
				assert.Equal(t, expectedSchedulable, actualResult.isSchedulable)
			}
		})
	}
}
