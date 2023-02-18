package scheduler

import (
	"context"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"
	"testing"
	"time"
)

func TestMetricsCollector_TestCollect(t *testing.T) {

	baseTime := time.Now().UTC()

	queuedJob := jobdb.NewJob(
		util.NewULID(),
		"testJobset",
		"testQueue",
		uint32(10),
		&schedulerobjects.JobSchedulingInfo{
			PriorityClassName: "test-priority",
			SubmitTime:        baseTime,
			ObjectRequirements: []*schedulerobjects.ObjectRequirements{
				{
					Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
						PodRequirements: testSmallCpuJob("test-queue", 1),
					},
				},
			},
		},
		false,
		false,
		false,
		baseTime.UnixNano())

	testQueue := &database.Queue{
		Name:   "testQueue",
		Weight: 100,
	}

	tests := map[string]struct {
		initialJobs  []*jobdb.Job
		poolMappings map[string]string
		queues       []*database.Queue
		expected     prometheus.Metric
	}{
		"foo": {
			initialJobs: []*jobdb.Job{queuedJob},
			queues:      []*database.Queue{testQueue},
			poolMappings: map[string]string{
				queuedJob.Id(): "test-pool",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			testClock := clock.NewFakeClock(baseTime)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// set up job db with initial jobs
			jobDb, err := jobdb.NewJobDb()
			require.NoError(t, err)
			txn := jobDb.WriteTxn()
			err = jobDb.Upsert(txn, tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			queueRepository := schedulermocks.NewMockQueueRepository(ctrl)
			queueRepository.EXPECT().GetAllQueues().Return(tc.queues, nil).Times(1)
			poolAssigner := &MockPoolAssigner{tc.poolMappings}

			collector := NewMetricsCollector(
				jobDb,
				queueRepository,
				poolAssigner,
				2*time.Second,
			)
			collector.clock = testClock
			collector.refresh(ctx)
			reg := prometheus.NewRegistry()
			reg.MustRegister(collector)
			result, err := reg.Gather()
			require.NoError(t, err)

			for _, r := range result {
				log.Infof("%+v", r)
			}
		})
	}
}

type MockPoolAssigner struct {
	poolsById map[string]string
}

func (m MockPoolAssigner) Refresh(ctx context.Context) error {
	return nil
}

func (m MockPoolAssigner) AssignPool(j *jobdb.Job) (string, error) {
	return m.poolsById[j.Id()], nil
}
