package scheduler

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/util"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/database"
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
	jobs := make([]*SchedulerJob, 10)
	for i := 0; i < 10; i++ {
		jobs[i] = OneCpuJob()
		jobs[i].Timestamp = int64(i) // ensure the jobs are in the order we expect
	}
	tests := map[string]struct {
		executors     []*database.Executor
		queues        []*database.Queue
		jobs          []*SchedulerJob
		perQueueLimit map[string]float64
		expectedJobs  map[string]string // map of jobId to name of executor on which it should be scheduled
	}{
		"fill up both clusters": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("0"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("0"), baseTime),
			},
			queues: []*database.Queue{&queue},
			jobs:   jobs,
			expectedJobs: map[string]string{
				jobs[0].JobId: "executor1",
				jobs[1].JobId: "executor1",
				jobs[2].JobId: "executor2",
				jobs[3].JobId: "executor2",
			},
		},
		"one executor stale": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("0"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("0"), baseTime.Add(-1*time.Hour)),
			},
			queues: []*database.Queue{&queue},
			jobs:   jobs,
			expectedJobs: map[string]string{
				jobs[0].JobId: "executor1",
				jobs[1].JobId: "executor1",
			},
		},
		"one executor full": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("2"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("0"), baseTime),
			},
			queues: []*database.Queue{&queue},
			jobs:   jobs,
			expectedJobs: map[string]string{
				jobs[0].JobId: "executor2",
				jobs[1].JobId: "executor2",
			},
		},
		"user is at usage cap before scheduling": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("2"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("0"), baseTime),
			},
			queues:        []*database.Queue{&queue},
			jobs:          jobs,
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs:  map[string]string{},
		},
		"user hits usage cap during scheduling": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("1"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("0"), baseTime),
			},
			queues:        []*database.Queue{&queue},
			jobs:          jobs,
			perQueueLimit: map[string]float64{"cpu": 0.5},
			expectedJobs: map[string]string{
				jobs[0].JobId: "executor1",
			},
		},
		"no jobs to schedule": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", resource.MustParse("2"), baseTime),
				TwoCoreExecutor("executor2", resource.MustParse("2"), baseTime),
			},
			queues:       []*database.Queue{&queue},
			jobs:         nil,
			expectedJobs: map[string]string{},
		},
		"no executor available": {
			executors:    []*database.Executor{},
			queues:       []*database.Queue{&queue},
			jobs:         jobs,
			expectedJobs: map[string]string{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			config := testSchedulingConfig()
			if tc.perQueueLimit != nil {
				config = withPerQueueLimits(tc.perQueueLimit, config)
			}
			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)

			mockExecutorRepo.EXPECT().GetExecutors().Return(tc.executors, nil).AnyTimes()
			mockQueueRepo.EXPECT().GetAllQueues().Return(tc.queues, nil).AnyTimes()

			algo := NewLegacySchedulingAlgo(config,
				mockExecutorRepo,
				mockQueueRepo)

			// Use a test clock so we can control time
			algo.clock = clock.NewFakeClock(baseTime)

			// Set up JobDb
			jobDb, err := NewJobDb()
			require.NoError(t, err)
			txn := jobDb.WriteTxn()
			err = jobDb.Upsert(txn, tc.jobs)
			require.NoError(t, err)

			scheduledJobs, err := algo.Schedule(txn, jobDb)
			require.NoError(t, err)

			// check that we have scheduled the jobs we expect
			assert.Equal(t, len(tc.expectedJobs), len(scheduledJobs))

			for _, job := range scheduledJobs {
				expectedExecutor, ok := tc.expectedJobs[job.JobId]
				assert.True(t, ok)
				assert.Equal(t, expectedExecutor, job.Executor)
				assert.Equal(t, false, job.Queued)
			}

			// check all scheduled jobs are up-to-date in db
			for _, job := range scheduledJobs {
				dbJob, err := jobDb.GetById(txn, job.JobId)
				require.NoError(t, err)
				assert.Equal(t, job, dbJob)
			}
		})
	}
}

func twoCoreNode(usedCpu resource.Quantity) *schedulerobjects.Node {
	allocatableCpu := resource.MustParse("2")
	(&allocatableCpu).Sub(usedCpu)
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			[]int32{0},
			map[string]resource.Quantity{
				"cpu":    allocatableCpu,
				"memory": resource.MustParse("256Gi"),
			},
		),
	}
}

func TwoCoreExecutor(name string, usedCPU resource.Quantity, updateTime time.Time) *database.Executor {
	return &database.Executor{
		Name: name,
		Pool: poolName,
		Usage: &schedulerobjects.ClusterResourceUsageReport{
			Pool: poolName,
			ResourcesByQueue: map[string]*schedulerobjects.QueueClusterResourceUsage{queueName: {
				Created:    updateTime,
				Queue:      queueName,
				ExecutorId: name,
				ResourcesByPriority: map[int32]schedulerobjects.ResourceList{
					0: {Resources: map[string]resource.Quantity{
						"cpu": usedCPU,
					}},
				},
			}},
		},
		Nodes:          []*schedulerobjects.Node{twoCoreNode(usedCPU)},
		LastUpdateTime: updateTime,
	}
}

func OneCpuJob() *SchedulerJob {
	return &SchedulerJob{
		JobId:    util.NewULID(),
		Queue:    queueName,
		Jobset:   "test-jobset",
		Queued:   true,
		Priority: 0,
		jobSchedulingInfo: &schedulerobjects.JobSchedulingInfo{
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
						},
					},
				},
			},
		},
	}
}
