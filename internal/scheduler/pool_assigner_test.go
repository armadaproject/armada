package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestPoolAssigner_AssignPools(t *testing.T) {
	queuedJob := testfixtures.TestQueuedJobDbJob()
	cpuExecutor := testfixtures.TestExecutor(testfixtures.BaseTime)
	runningJob := queuedJob.
		WithQueued(false).
		WithNewRun(cpuExecutor.Id, "testNode", "testNode", 0)

	tests := map[string]struct {
		executorTimout time.Duration
		executors      []*schedulerobjects.Executor
		job            *jobdb.Job
		expectedPools  []string
	}{
		"queued job with single pool": {
			job:           queuedJob.WithPools([]string{"cpu"}),
			expectedPools: []string{"cpu"},
		},
		"queued job with multiple pools": {
			job:           queuedJob.WithPools([]string{"cpu", "gpu"}),
			expectedPools: []string{"cpu", "gpu"},
		},
		"running job matches pool": {
			executors:     []*schedulerobjects.Executor{cpuExecutor},
			job:           runningJob,
			expectedPools: []string{"cpu"},
		},
		"running job doesn't match pool": {
			executors:     []*schedulerobjects.Executor{},
			job:           runningJob,
			expectedPools: []string{""},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			assigner, err := NewPoolAssigner(mockExecutorRepo)
			require.NoError(t, err)

			err = assigner.Refresh(ctx)
			require.NoError(t, err)
			pools, err := assigner.AssignPools(tc.job)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedPools, pools)
		})
	}
}
