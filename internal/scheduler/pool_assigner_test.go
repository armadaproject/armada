package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/stretchr/testify/require"
)

func TestPoolAssigner_AssignPool(t *testing.T) {
	executorTimeout := 15 * time.Minute
	cpuJob := testQueuedJobDbJob()
	gpuJob := WithJobDbJobPodRequirements(testQueuedJobDbJob(), testGpuJob(testQueue, testPriorities[0]))

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		job            *jobdb.Job
		expectedPool   string
	}{
		"matches pool": {
			executorTimout: executorTimeout,
			config:         testSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(baseTime)},
			job:            cpuJob,
			expectedPool:   "cpu",
		},
		"doesn't match pool": {
			executorTimout: executorTimeout,
			config:         testSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(baseTime)},
			job:            gpuJob,
			expectedPool:   "",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			fakeClock := clock.NewFakeClock(baseTime)
			assigner := NewPoolAssigner(tc.executorTimout, tc.config, mockExecutorRepo)
			assigner.clock = fakeClock

			err := assigner.Refresh(ctx)
			require.NoError(t, err)
			pool, err := assigner.AssignPool(tc.job)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedPool, pool)
		})
	}
}
