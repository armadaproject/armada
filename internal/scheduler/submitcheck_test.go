package scheduler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestSubmitChecker_CheckJobDbJobs(t *testing.T) {
	defaultTimeout := 15 * time.Minute
	baseTime := time.Now().UTC()
	expiredTime := baseTime.Add(-defaultTimeout).Add(-1 * time.Second)

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		job            *jobdb.Job
		expectPass     bool
	}{
		"one job schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			job:            testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     true,
		},
		"no jobs schedule due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			job:            testfixtures.Test32Cpu256GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     false,
		},
		"no jobs schedule due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(baseTime)},
			job:            testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)),
			expectPass:     false,
		},
		"no jobs schedule due to executor timeout": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(expiredTime)},
			job:            testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     false,
		},
		"multiple executors, 1 expired": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(expiredTime), testfixtures.TestExecutor(baseTime)},
			job:            testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     true,
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
			submitCheck := NewSubmitChecker(tc.executorTimout, tc.config, mockExecutorRepo)
			submitCheck.clock = fakeClock
			submitCheck.updateExecutors(ctx)
			isSchedulable, reason := submitCheck.CheckJobDbJobs([]*jobdb.Job{tc.job})
			assert.Equal(t, tc.expectPass, isSchedulable)
			if !tc.expectPass {
				assert.NotEqual(t, "", reason)
			}
			logrus.Info(reason)
		})
	}
}

func TestSubmitChecker_TestCheckApiJobs(t *testing.T) {
	defaultTimeout := 15 * time.Minute
	testfixtures.BaseTime = time.Now().UTC()
	expiredTime := testfixtures.BaseTime.Add(-defaultTimeout).Add(-1 * time.Second)

	tests := map[string]struct {
		executorTimout time.Duration
		config         configuration.SchedulingConfig
		executors      []*schedulerobjects.Executor
		jobs           []*api.Job
		expectPass     bool
	}{
		"one job schedules": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJob()},
			expectPass:     true,
		},
		"multiple jobs schedule": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJob(), testfixtures.Test1CoreCpuApiJob()},
			expectPass:     true,
		},
		"first job schedules, second doesn't": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJob(), testfixtures.Test100CoreCpuApiJob()},
			expectPass:     false,
		},
		"no jobs schedule due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test100CoreCpuApiJob()},
			expectPass:     false,
		},
		"no jobs schedule due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJobWithNodeSelector(map[string]string{"foo": "bar"})},
			expectPass:     false,
		},
		"no jobs schedule due to executor timeout": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(expiredTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJob()},
			expectPass:     false,
		},
		"multiple executors, 1 expired": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(expiredTime), testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{testfixtures.Test1CoreCpuApiJob()},
			expectPass:     true,
		},
		"gang job all jobs fit": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           testfixtures.TestNApiJobGang(5),
			expectPass:     true,
		},
		"gang job all jobs don't fit": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           testfixtures.TestNApiJobGang(100),
			expectPass:     false,
		},
		"Less than min cardinality gang jobs in a batch skips submit check": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testfixtures.TestExecutor(testfixtures.BaseTime)},
			jobs:           testfixtures.TestNApiJobGangLessThanMinCardinality(5),
			expectPass:     true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			fakeClock := clock.NewFakeClock(testfixtures.BaseTime)
			submitCheck := NewSubmitChecker(tc.executorTimout, tc.config, mockExecutorRepo)
			submitCheck.clock = fakeClock
			submitCheck.updateExecutors(ctx)
			result, msg := submitCheck.CheckApiJobs(tc.jobs)
			assert.Equal(t, tc.expectPass, result)
			if !tc.expectPass {
				assert.NotEqual(t, "", msg)
			}
			logrus.Info(msg)
		})
	}
}
