package scheduler

import (
	"context"
	commmonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"
	"testing"
	"time"
)

func TestMetricsCollector_TestCollect(t *testing.T) {

	baseTime := time.Now().UTC()

	testQueue := &database.Queue{
		Name:   "testQueue",
		Weight: 100,
	}

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
						PodRequirements: testSmallCpuJob(testQueue.Name, 1),
					},
				},
			},
		},
		false,
		false,
		false,
		baseTime.UnixNano())

	tests := map[string]struct {
		initialJobs  []*jobdb.Job
		poolMappings map[string]string
		queues       []*database.Queue
		expected     []prometheus.Metric
	}{
		"queued metrics": {
			initialJobs: []*jobdb.Job{queuedJob},
			queues:      []*database.Queue{testQueue},
			poolMappings: map[string]string{
				queuedJob.Id(): "test-pool",
			},
			expected: []prometheus.Metric{
				prometheus.MustNewConstMetric(commmonmetrics.QueueSizeDesc, prometheus.GaugeValue, 1.0, testQueue.Name),
				prometheus.MustNewConstHistogram(
					commmonmetrics.QueueDurationDesc, 1, 1, map[float64]uint64{}, "test-pool", "test-priority", testQueue.Name),
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
			metricChan := make(chan prometheus.Metric, 1000)
			collector.Collect(metricChan)
			close(metricChan)
			actual := make([]prometheus.Metric, 0)
			for m := range metricChan {
				actual = append(actual, m)
			}
			require.NoError(t, err)

			assert.Equal(t, tc.expected[1], actual[1])
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
