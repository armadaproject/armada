package scheduler

import (
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/context"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestMetricsCollector_TestCollect_QueueMetrics(t *testing.T) {
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

			executorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepository.EXPECT().GetExecutors(ctx).Return([]*schedulerobjects.Executor{}, nil)

			collector := NewMetricsCollector(
				jobDb,
				queueRepository,
				executorRepository,
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

func TestMetricsCollector_TestCollect_ClusterMetrics(t *testing.T) {
	executor := createExecutor("cluster-1", createNode("type-1"), createNode("type-1"))
	executorWithMultipleNodeTypes := createExecutor("cluster-1", createNode("type-1"), createNode("type-2"))

	unschedulableNode := createNode("type-1")
	unschedulableNode.Unschedulable = true

	executorWithUnschedulableNodes := createExecutor("cluster-1", createNode("type-1"), unschedulableNode)

	job1 := testfixtures.TestRunningJobDbJob(0)
	job2 := testfixtures.TestRunningJobDbJob(0)
	nodeWithJobs := createNode("type-1")
	nodeWithJobs.StateByJobRunId[job1.LatestRun().Id().String()] = schedulerobjects.JobRunState_PENDING
	nodeWithJobs.StateByJobRunId[job2.LatestRun().Id().String()] = schedulerobjects.JobRunState_RUNNING
	nodeWithJobs.ResourceUsageByQueue[testfixtures.TestQueue] = &schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
	}
	executorWithJobs := createExecutor("cluster-1", nodeWithJobs)

	tests := map[string]struct {
		jobDbJobs []*jobdb.Job
		executors []*schedulerobjects.Executor
		expected  []prometheus.Metric
	}{
		"empty cluster single node type": {
			jobDbJobs: []*jobdb.Job{},
			executors: []*schedulerobjects.Executor{executor},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(2, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(2, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
			},
		},
		"empty cluster multi node type": {
			jobDbJobs: []*jobdb.Job{},
			executors: []*schedulerobjects.Executor{executorWithMultipleNodeTypes},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-2"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-2"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-2"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-2"),
				commonmetrics.NewClusterTotalCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-2"),
				commonmetrics.NewClusterTotalCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-2"),
			},
		},
		"empty cluster with unschedulable node": {
			jobDbJobs: []*jobdb.Job{},
			executors: []*schedulerobjects.Executor{executorWithUnschedulableNodes},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(2, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
			},
		},
		"cluster with jobs": {
			jobDbJobs: []*jobdb.Job{job1, job2},
			executors: []*schedulerobjects.Executor{executorWithJobs},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueLeasedPodCount(1, "cluster-1", testfixtures.TestPool, testfixtures.TestQueue, "Pending", "type-1"),
				commonmetrics.NewQueueLeasedPodCount(1, "cluster-1", testfixtures.TestPool, testfixtures.TestQueue, "Running", "type-1"),
				commonmetrics.NewQueueAllocated(2, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewQueueAllocated(2*1024*1024*1024, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewQueueUsed(1, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewQueueUsed(1*1024*1024*1024, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
			},
		},
		"jobs missing from jobDb": {
			jobDbJobs: []*jobdb.Job{},
			executors: []*schedulerobjects.Executor{executorWithJobs},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueUsed(1, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewQueueUsed(1*1024*1024*1024, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
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
			err := jobDb.Upsert(txn, tc.jobDbJobs)
			require.NoError(t, err)
			txn.Commit()

			queueRepository := schedulermocks.NewMockQueueRepository(ctrl)
			queueRepository.EXPECT().GetAllQueues().Return([]*database.Queue{}, nil).Times(1)
			poolAssigner := &MockPoolAssigner{testfixtures.TestPool, map[string]string{}}

			executorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepository.EXPECT().GetExecutors(ctx).Return(tc.executors, nil)

			collector := NewMetricsCollector(
				jobDb,
				queueRepository,
				executorRepository,
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
			require.Equal(t, len(actual), len(tc.expected))
			for i := 0; i < len(tc.expected); i++ {
				a1 := actual[i]
				// As resources are a map, the ordering isn't deterministic, so we have to use compare
				// Alternatively if we can work out how to sort prometheus.Metric we could do that instead
				assert.Contains(t, tc.expected, a1)
			}
		})
	}
}

func createExecutor(clusterName string, nodes ...*schedulerobjects.Node) *schedulerobjects.Executor {
	return &schedulerobjects.Executor{
		Id:    clusterName,
		Pool:  testfixtures.TestPool,
		Nodes: nodes,
	}
}

func createNode(nodeType string) *schedulerobjects.Node {
	node := testfixtures.Test32CpuNode([]int32{})
	node.ReportingNodeType = nodeType
	node.StateByJobRunId = map[string]schedulerobjects.JobRunState{}
	node.ResourceUsageByQueue = map[string]*schedulerobjects.ResourceList{}
	return node
}

type MockPoolAssigner struct {
	defaultPool string
	poolsById   map[string]string
}

func (m MockPoolAssigner) Refresh(_ *context.ArmadaContext) error {
	return nil
}

func (m MockPoolAssigner) AssignPool(j *jobdb.Job) (string, error) {
	pool, ok := m.poolsById[j.Id()]
	if !ok {
		pool = m.defaultPool
	}
	return pool, nil
}
