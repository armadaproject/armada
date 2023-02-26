package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/clock"

	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
)

func TestMetricsCollector_TestCollect(t *testing.T) {
	gb := float64(1024 * 1024 * 1024)
	queuedJobs := make([]*jobdb.Job, 3)
	runningJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		startTime := baseTime.Add(-time.Duration(100*i) * time.Second).UnixNano()
		queuedJobs[i] = testQueuedJobDbJob().WithCreated(startTime)
		runningJobs[i] = testRunningJobDbJob(startTime)
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
			queues:      []*database.Queue{testDbQueue()},
			defaultPool: testPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(3.0, testQueue),
				commonmetrics.NewQueueDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinQueueDuration(0, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMaxQueueDuration(200, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMedianQueueDuration(100, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMinQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMaxQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMedianQueueResources(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewCountQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewQueueResources(3*gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMinQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMaxQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMedianQueueResources(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewCountQueueResources(3, testPool, testDefaultPriorityClass, testQueue, "memory"),
			},
		},
		"running metrics": {
			initialJobs: runningJobs,
			queues:      []*database.Queue{testDbQueue()},
			defaultPool: testPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(0.0, testQueue),
				commonmetrics.NewJobRunRunDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinJobRunDuration(0, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMaxJobRunDuration(200, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMedianJobRunDuration(100, testPool, testDefaultPriorityClass, testQueue),
				commonmetrics.NewMinQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMaxQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMedianQueueAllocated(1, testPool, testDefaultPriorityClass, testQueue, "cpu"),
				commonmetrics.NewMinQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMaxQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
				commonmetrics.NewMedianQueueAllocated(gb, testPool, testDefaultPriorityClass, testQueue, "memory"),
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
			jobDb := jobdb.NewJobDb()
			txn := jobDb.WriteTxn()
			err := jobDb.Upsert(txn, tc.initialJobs)
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
			metricChan := make(chan prometheus.Metric, 1000) // large buffer so we don't block
			collector.Collect(metricChan)
			close(metricChan)
			actual := make([]prometheus.Metric, 0)
			for m := range metricChan {
				actual = append(actual, m)
			}
			require.NoError(t, err)
			for i := 0; i < len(tc.expected); i++ {
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
