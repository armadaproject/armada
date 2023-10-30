package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
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
			executors:      []*schedulerobjects.Executor{testExecutor(baseTime)},
			job:            testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     true,
		},
		"no jobs schedule due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(baseTime)},
			job:            testfixtures.Test32Cpu256GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     false,
		},
		"no jobs schedule due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(baseTime)},
			job:            testfixtures.WithNodeSelectorJob(map[string]string{"foo": "bar"}, testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)),
			expectPass:     false,
		},
		"no jobs schedule due to executor timeout": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(expiredTime)},
			job:            testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1),
			expectPass:     false,
		},
		"multiple executors, 1 expired": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(expiredTime), testExecutor(baseTime)},
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
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test1CoreCpuJob()},
			expectPass:     true,
		},
		"multiple jobs schedule": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test1CoreCpuJob(), test1CoreCpuJob()},
			expectPass:     true,
		},
		"first job schedules, second doesn't": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test1CoreCpuJob(), test100CoreCpuJob()},
			expectPass:     false,
		},
		"no jobs schedule due to resources": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test100CoreCpuJob()},
			expectPass:     false,
		},
		"no jobs schedule due to selector": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test1CoreCpuJobWithNodeSelector(map[string]string{"foo": "bar"})},
			expectPass:     false,
		},
		"no jobs schedule due to executor timeout": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(expiredTime)},
			jobs:           []*api.Job{test1CoreCpuJob()},
			expectPass:     false,
		},
		"multiple executors, 1 expired": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(expiredTime), testExecutor(testfixtures.BaseTime)},
			jobs:           []*api.Job{test1CoreCpuJob()},
			expectPass:     true,
		},
		"gang job all jobs fit": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           testNJobGang(5),
			expectPass:     true,
		},
		"gang job all jobs don't fit": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           testNJobGang(100),
			expectPass:     false,
		},
		"Less than min cardinality gang jobs in a batch skips submit check": {
			executorTimout: defaultTimeout,
			config:         testfixtures.TestSchedulingConfig(),
			executors:      []*schedulerobjects.Executor{testExecutor(testfixtures.BaseTime)},
			jobs:           testNJobGangLessThanMinCardinality(5),
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

// TODO: Move to testfixtures_test.go/delete in favour of existing fixture.
func test1CoreCpuJob() *api.Job {
	return &api.Job{
		Id:    util.NewULID(),
		Queue: uuid.NewString(),
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: map[v1.ResourceName]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
						Requests: map[v1.ResourceName]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}
}

// TODO: Move to testfixtures_test.go.
func testNJobGang(n int) []*api.Job {
	gangId := uuid.NewString()
	gang := make([]*api.Job, n)
	for i := 0; i < n; i++ {
		job := test1CoreCpuJob()
		job.Annotations = map[string]string{
			configuration.GangIdAnnotation:                 gangId,
			configuration.GangCardinalityAnnotation:        fmt.Sprintf("%d", n),
			configuration.GangMinimumCardinalityAnnotation: fmt.Sprintf("%d", n),
		}
		gang[i] = job
	}
	return gang
}

// TODO: Move to testfixtures_test.go.
func testNJobGangLessThanMinCardinality(n int) []*api.Job {
	gangId := uuid.NewString()
	gang := make([]*api.Job, n)
	for i := 0; i < n; i++ {
		job := test1CoreCpuJob()
		job.Annotations = map[string]string{
			configuration.GangIdAnnotation:                 gangId,
			configuration.GangCardinalityAnnotation:        fmt.Sprintf("%d", n+2),
			configuration.GangMinimumCardinalityAnnotation: fmt.Sprintf("%d", n+1),
		}
		gang[i] = job
	}
	return gang
}

// TODO: Move to testfixtures_test.go.
func test100CoreCpuJob() *api.Job {
	job := test1CoreCpuJob()
	hundredCores := map[v1.ResourceName]resource.Quantity{
		"cpu": resource.MustParse("100"),
	}
	job.PodSpec.Containers[0].Resources.Limits = hundredCores
	job.PodSpec.Containers[0].Resources.Requests = hundredCores
	return job
}

// TODO: Move to testfixtures_test.go.
func test1CoreCpuJobWithNodeSelector(selector map[string]string) *api.Job {
	job := test1CoreCpuJob()
	job.PodSpec.NodeSelector = selector
	return job
}

// TODO: Move to testfixtures_test.go.
func testExecutor(lastUpdateTime time.Time) *schedulerobjects.Executor {
	return &schedulerobjects.Executor{
		Id:             uuid.NewString(),
		Pool:           "cpu",
		LastUpdateTime: lastUpdateTime,
		Nodes:          testfixtures.TestCluster(),
	}
}
