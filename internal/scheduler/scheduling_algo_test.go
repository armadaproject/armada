package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

const (
	queueName = "queue1"
	poolName  = "pool1"
)

var (
	queue = database.Queue{
		Name:   queueName,
		Weight: 100,
	}
	baseTime time.Time
)

func TestLegacySchedulingAlgo_TestSchedule(t *testing.T) {
	queuedJobs := make([]*jobdb.Job, 10)
	for i := 0; i < 10; i++ {
		queuedJobs[i] = OneCpuJob(int64(i)) // ensure the queuedJobs are in the order we expect
	}
	runningJobs := []*jobdb.Job{OneCoreRunningJob(1, "executor1"), OneCoreRunningJob(1, "executor1")}
	tests := map[string]struct {
		executors     []*schedulerobjects.Executor
		queues        []*database.Queue
		queuedJobs    []*jobdb.Job
		runningJobs   []*jobdb.Job
		perQueueLimit map[string]float64
		expectedJobs  map[string]string // map of jobId to name of executor on which it should be scheduled
	}{
		"fill up both clusters": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", nil, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
			},
			queues:     []*database.Queue{&queue},
			queuedJobs: queuedJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
				queuedJobs[1].Id(): "executor1",
				queuedJobs[2].Id(): "executor2",
				queuedJobs[3].Id(): "executor2",
			},
		},
		"one executor stale": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", nil, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime.Add(-1*time.Hour)),
			},
			queues:     []*database.Queue{&queue},
			queuedJobs: queuedJobs,
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
				queuedJobs[1].Id(): "executor1",
			},
		},
		"one executor full": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", runningJobs, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
			},
			queues:      []*database.Queue{&queue},
			queuedJobs:  queuedJobs,
			runningJobs: []*jobdb.Job{runningJobs[0], runningJobs[1]},
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor2",
				queuedJobs[1].Id(): "executor2",
			},
		},
		"user is at usage cap before scheduling": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", runningJobs, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
			},
			queues:        []*database.Queue{&queue},
			queuedJobs:    queuedJobs,
			runningJobs:   []*jobdb.Job{runningJobs[0], runningJobs[1]},
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs:  map[string]string{},
		},
		"user hits usage cap during scheduling": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", []*jobdb.Job{runningJobs[0]}, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
			},
			queues:        []*database.Queue{&queue},
			queuedJobs:    queuedJobs,
			runningJobs:   []*jobdb.Job{runningJobs[0]},
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs: map[string]string{
				queuedJobs[0].Id(): "executor1",
			},
		},
		"no queuedJobs to schedule": {
			executors: []*schedulerobjects.Executor{
				TwoCoreExecutor("executor1", nil, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
			},
			queues:       []*database.Queue{&queue},
			queuedJobs:   nil,
			expectedJobs: map[string]string{},
		},
		"no executor available": {
			executors:    []*schedulerobjects.Executor{},
			queues:       []*database.Queue{&queue},
			queuedJobs:   queuedJobs,
			expectedJobs: map[string]string{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			config := testSchedulingConfig()
			if tc.perQueueLimit != nil {
				config = withPerQueueLimitsConfig(tc.perQueueLimit, config)
			}
			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)

			mockExecutorRepo.EXPECT().GetExecutors(ctx).Return(tc.executors, nil).AnyTimes()
			mockQueueRepo.EXPECT().GetAllQueues().Return(tc.queues, nil).AnyTimes()

			algo := NewLegacySchedulingAlgo(config,
				mockExecutorRepo,
				mockQueueRepo)

			// Use a test clock so we can control time
			algo.clock = clock.NewFakeClock(baseTime)

			// Set up JobDb
			jobDb, err := jobdb.NewJobDb()
			require.NoError(t, err)
			txn := jobDb.WriteTxn()
			err = jobDb.Upsert(txn, append(tc.queuedJobs, tc.runningJobs...))
			require.NoError(t, err)

			scheduledJobs, err := algo.Schedule(ctx, txn, jobDb)
			require.NoError(t, err)

			// check that we have scheduled the queuedJobs we expect
			assert.Equal(t, len(tc.expectedJobs), len(scheduledJobs))

			for _, job := range scheduledJobs {
				expectedExecutor, ok := tc.expectedJobs[job.Id()]
				require.True(t, ok)
				run := job.LatestRun()
				require.NotEqual(t, t, run)
				assert.Equal(t, expectedExecutor, run.Executor())
				assert.Equal(t, false, job.Queued())
			}

			// check all scheduled queuedJobs are up-to-date in db
			for _, job := range scheduledJobs {
				dbJob, err := jobDb.GetById(txn, job.Id())
				require.NoError(t, err)
				assert.Equal(t, job, dbJob)
			}
		})
	}
}

func twoCoreNode(jobs []*jobdb.Job) *schedulerobjects.Node {
	usedCpu := resource.MustParse("0")
	for _, job := range jobs {
		cpuReq := job.
			JobSchedulingInfo().
			ObjectRequirements[0].
			GetPodRequirements().
			GetResourceRequirements().Limits["cpu"]
		usedCpu.Add(cpuReq)
	}
	allocatableCpu := resource.MustParse("2")
	(&allocatableCpu).Sub(usedCpu)
	id := uuid.NewString()
	jobRunsByState := make(map[string]schedulerobjects.JobRunState, len(jobs))
	for _, job := range jobs {
		jobRunsByState[job.LatestRun().Id().String()] = schedulerobjects.JobRunState_RUNNING
	}
	return &schedulerobjects.Node{
		Id: id,
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		Labels: map[string]string{
			testHostnameLabel: id,
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			[]int32{0},
			map[string]resource.Quantity{
				"cpu":    allocatableCpu,
				"memory": resource.MustParse("256Gi"),
			},
		),
		JobRunsByState: jobRunsByState,
	}
}

func TwoCoreExecutor(name string, jobs []*jobdb.Job, updateTime time.Time) *schedulerobjects.Executor {
	return &schedulerobjects.Executor{
		Id:             name,
		Pool:           poolName,
		Nodes:          []*schedulerobjects.Node{twoCoreNode(jobs)},
		LastUpdateTime: updateTime,
	}
}

func OneCpuJob(creationTime int64) *jobdb.Job {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						ResourceRequirements: v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"memory": resource.MustParse("1Mi"),
								"cpu":    resource.MustParse("1"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"memory": resource.MustParse("1"),
								"cpu":    resource.MustParse("1"),
							},
						},
						Annotations: map[string]string{
							JobIdAnnotation: uuid.NewString(),
						},
					},
				},
			},
		},
	}
	return jobdb.NewJob(
		util.NewULID(),
		"testJobset",
		queueName,
		0,
		schedulingInfo,
		false,
		false,
		false,
		creationTime).WithQueued(true)
}

func OneCoreRunningJob(creationTime int64, executor string) *jobdb.Job {
	return OneCpuJob(creationTime).WithNewRun(executor)
}
