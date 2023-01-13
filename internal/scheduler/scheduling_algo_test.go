//go:generate mockgen -destination=./mocks/mock_repositories.go -package=schedulermocks "github.com/G-Research/armada/internal/scheduler/database" ExecutorRepository,QueueRepository

package scheduler

import (
	commonutil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/database"
	schedulermocks "github.com/G-Research/armada/internal/scheduler/mocks"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
	"time"
)

const (
	queueName = "queue1"
	poolName  = "pool1"
	priority  = 100
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
	}
	tests := map[string]struct {
		executors    []*database.Executor
		queues       []*database.Queue
		jobs         []*SchedulerJob
		expectedJobs map[string]string // map of jobId to name of executor on which it should be scheduled
	}{
		"schedule on both clusters": {
			executors: []*database.Executor{
				TwoCoreExecutor("executor1", nil, baseTime),
				TwoCoreExecutor("executor2", nil, baseTime),
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
		"one executor stale":    {},
		"one executor full":     {},
		"user hits usage cap":   {},
		"no jobs to schedule":   {},
		"no executor available": {},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			config := testSchedulingConfig()
			ctrl := gomock.NewController(t)
			mockExecutorRepo := schedulermocks.NewMockExecutorRepository(ctrl)
			mockQueueRepo := schedulermocks.NewMockQueueRepository(ctrl)

			mockExecutorRepo.EXPECT().GetExecutors().Return(tc.executors, nil).AnyTimes()
			mockQueueRepo.EXPECT().GetAllQueues().Return(tc.queues, nil).AnyTimes()

			algo := NewLegacySchedulingAlgo(config,
				mockExecutorRepo,
				mockQueueRepo)

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
				assert.Equal(t, 1, len(job.Runs))
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

func twoCoreNode() *schedulerobjects.Node {
	return &schedulerobjects.Node{
		Id: uuid.NewString(),
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		AllocatableByPriorityAndResource: schedulerobjects.NewAllocatableByPriorityAndResourceType(
			[]int32{priority},
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("256Gi"),
			},
		),
	}
}

func TwoCoreExecutor(name string, usage map[int32]schedulerobjects.ResourceList, updateTime time.Time) *database.Executor {
	return &database.Executor{
		Name: name,
		Pool: poolName,
		Usage: &schedulerobjects.ClusterResourceUsageReport{
			Pool: poolName,
			ResourcesByQueue: map[string]*schedulerobjects.QueueClusterResourceUsage{queueName: {
				Created:             updateTime,
				Queue:               queueName,
				ExecutorId:          name,
				ResourcesByPriority: usage,
			}},
		},
		Nodes:          []*schedulerobjects.Node{twoCoreNode()},
		LastUpdateTime: updateTime,
	}
}

func OneCpuJob() *SchedulerJob {
	return &SchedulerJob{
		JobId:    commonutil.NewULID(),
		Queue:    queueName,
		Jobset:   "test-jobset",
		Queued:   true,
		Priority: priority,
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
