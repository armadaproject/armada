package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestMetricsCollector_TestCollect(t *testing.T) {
	baseTime := time.Now().UTC()

	testQueue := &database.Queue{
		Name:   "testQueue",
		Weight: 100,
	}

	queuedJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		queuedJobs[i] = jobdb.NewJob(
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
			baseTime.Add(-time.Duration(100*i)*time.Second).UnixNano())
	}

	runningJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		startTime := baseTime.Add(-time.Duration(100*i) * time.Second).UnixNano()
		runningJobs[i] = queuedJobs[i].WithQueued(false).WithUpdatedRun(jobdb.MinimalRun(uuid.New(), startTime))
	}

	tests := map[string]struct {
		initialJobs  []*jobdb.Job
		defaultPool  string
		poolMappings map[string]string
		queues       []*database.Queue
		expected     []prometheus.Metric
	}{
		"queued metrics": {
			initialJobs: queuedJobs,
			queues:      []*database.Queue{testQueue},
			defaultPool: "test-pool",
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(3.0, testQueue.Name),
				commonmetrics.NewQueueDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					"test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMinQueueDuration(0, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMaxQueueDuration(200, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMedianQueueDuration(100, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewQueueResources(3, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMinQueueResources(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMaxQueueResources(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMedianQueueResources(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewCountQueueResources(3, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewQueueResources(1024*1024*1024*12, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewMinQueueResources(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewMaxQueueResources(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewMedianQueueResources(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewCountQueueResources(3, "test-pool", "test-priority", testQueue.Name, "memory"),
			},
		},
		"running metrics": {
			initialJobs: runningJobs,
			queues:      []*database.Queue{testQueue},
			defaultPool: "test-pool",
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(0.0, testQueue.Name),
				commonmetrics.NewJobRunRunDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					"test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMinJobRunDuration(0, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMaxJobRunDuration(200, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMedianJobRunDuration(100, "test-pool", "test-priority", testQueue.Name),
				commonmetrics.NewMinQueueAllocated(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMaxQueueAllocated(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMedianQueueAllocated(1, "test-pool", "test-priority", testQueue.Name, "cpu"),
				commonmetrics.NewMinQueueAllocated(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewMaxQueueAllocated(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
				commonmetrics.NewMedianQueueAllocated(1024*1024*1024*4, "test-pool", "test-priority", testQueue.Name, "memory"),
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
			poolAssigner := &MockPoolAssigner{tc.defaultPool, tc.poolMappings}

			collector := NewMetricsCollector(
				jobDb,
				queueRepository,
				poolAssigner,
				2*time.Second,
			)
			collector.clock = testClock
			err = collector.refresh(ctx)
			require.NoError(t, err)
			metricChan := make(chan prometheus.Metric, 1000)
			collector.Collect(metricChan)
			close(metricChan)
			actual := make([]prometheus.Metric, 0)
			for m := range metricChan {
				actual = append(actual, m)
			}
			require.NoError(t, err)
			for i := 0; i < len(tc.expected); i++ {
				println(i)
				a1 := actual[i]
				e1 := tc.expected[i]
				require.Equal(t, e1, a1)
			}
		})
	}
}

type MockPoolAssigner struct {
	defaultPool string
	poolsById   map[string]string
}

func (m MockPoolAssigner) Refresh(_ context.Context) error {
	return nil
}

func (m MockPoolAssigner) AssignPool(j *jobdb.Job) (string, error) {
	pool, ok := m.poolsById[j.Id()]
	if !ok {
		pool = m.defaultPool
	}
	return pool, nil
}
