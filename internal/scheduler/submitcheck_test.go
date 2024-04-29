package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
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

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		jobs           []*jobdb.Job
		expectedResult map[string]bool
	}{
		"one job schedulable": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1},
			expectedResult: map[string]bool{smallJob1.Id(): true},
		},
		"two jobs schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, smallJob2},
			expectedResult: map[string]bool{smallJob1.Id(): true, smallJob2.Id(): true},
		},
		"one job schedulable, one not due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{smallJob1, largeJob1},
			expectedResult: map[string]bool{smallJob1.Id(): true, largeJob1.Id(): false},
		},
		"no jobs schedulable due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{largeJob1},
			expectedResult: map[string]bool{largeJob1.Id(): false},
		},
		"no jobs schedulable due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			jobs:           []*jobdb.Job{testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, smallJob1)},
			expectedResult: map[string]bool{smallJob1.Id(): false},
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
			submitCheck := NewSubmitChecker(tc.config, mockExecutorRepo)
			submitCheck.clock = fakeClock
			submitCheck.updateExecutors(ctx)
			results, err := submitCheck.Check(tc.jobs)
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
