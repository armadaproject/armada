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
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestMetricsCollector_TestCollect(t *testing.T) {
	gb := float64(1024 * 1024 * 1024)
	queuedJobs := make([]*jobdb.Job, 3)
	runningJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		startTime := testfixtures.BaseTime.Add(-time.Duration(100*i) * time.Second).UnixNano()
		queuedJobs[i] = testfixtures.TestQueuedJobDbJob().WithCreated(startTime)
		runningJobs[i] = testfixtures.TestRunningJobDbJob(startTime)
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
			queues:      []*database.Queue{testfixtures.TestDbQueue()},
			defaultPool: testfixtures.TestPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(3.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueueDuration(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueueDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueueDuration(100, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewQueueResources(3, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMinQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMaxQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMedianQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewCountQueueResources(3, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewQueueResources(3*gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMinQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMaxQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMedianQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewCountQueueResources(3, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
			},
		},
		"running metrics": {
			initialJobs: runningJobs,
			queues:      []*database.Queue{testfixtures.TestDbQueue()},
			defaultPool: testfixtures.TestPool,
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(0.0, testfixtures.TestQueue),
				commonmetrics.NewJobRunRunDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinJobRunDuration(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxJobRunDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianJobRunDuration(100, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueueAllocated(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMaxQueueAllocated(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMedianQueueAllocated(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMinQueueAllocated(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMaxQueueAllocated(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMedianQueueAllocated(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			testClock := clock.NewFakeClock(testfixtures.BaseTime)
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
				nil,
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
