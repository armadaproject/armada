package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestMetricsCollector_TestCollect_QueueMetrics(t *testing.T) {
	gb := float64(1024 * 1024 * 1024)
	queuedJobs := make([]*jobdb.Job, 3)
	runningJobs := make([]*jobdb.Job, 3)
	for i := 0; i < len(queuedJobs); i++ {
		startTime := testfixtures.BaseTime.Add(-time.Duration(100*i) * time.Second).UnixNano()
		queuedJobs[i] = testfixtures.TestQueuedJobDbJob().WithCreated(startTime).WithPoolBidPrices(map[string]float64{testfixtures.TestPool: float64(i)})
		runningJobs[i] = testfixtures.TestRunningJobDbJob(startTime).WithPoolBidPrices(map[string]float64{testfixtures.TestPool: float64(i) + 100})
	}

	// Run that has been returned
	runStartTime := testfixtures.BaseTime.Add(-time.Duration(400) * time.Second).UnixNano()
	runTerminatedTime := testfixtures.BaseTime.Add(-time.Duration(200) * time.Second)
	run := jobdb.MinimalRun(uuid.New().String(), runStartTime)
	run = run.WithFailed(true)
	run = run.WithReturned(true)
	run = run.WithTerminatedTime(&runTerminatedTime)

	jobCreationTime := testfixtures.BaseTime.Add(-time.Duration(500) * time.Second).UnixNano()
	jobWithTerminatedRun := testfixtures.TestQueuedJobDbJob().WithCreated(jobCreationTime).WithUpdatedRun(run)

	queue := testfixtures.MakeTestQueue()
	queue.Labels = map[string]string{"foo": "bar"}

	tests := map[string]struct {
		initialJobs []*jobdb.Job
		queues      []*api.Queue
		expected    []prometheus.Metric
	}{
		"queued metrics": {
			initialJobs: queuedJobs,
			queues:      []*api.Queue{queue},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(3.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDistinctSchedulingKeyMetric(1.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueueDuration(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueueDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueueDuration(100, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueuePriceQueuedMetric(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueuePriceQueuedMetric(2, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueuePriceQueuedMetric(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
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
				commonmetrics.NewQueuePriorityMetric(100, testfixtures.TestQueue),
				commonmetrics.NewQueueLabelsMetric(testfixtures.TestQueue, map[string]string{"foo": "bar"}),
			},
		},
		"queued metrics for requeued job": {
			// This job was been requeued and has a terminated run
			// The queue duration stats should count from the time the last run finished instead of job creation time
			initialJobs: []*jobdb.Job{jobWithTerminatedRun},
			queues:      []*api.Queue{queue},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(1.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDistinctSchedulingKeyMetric(1.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDuration(1, 200,
					map[float64]uint64{60: 0, 600: 1, 1800: 1, 3600: 1, 10800: 1, 43200: 1, 86400: 1, 172800: 1, 604800: 1},
					testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueueDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueueDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueueDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueuePriceQueuedMetric(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueuePriceQueuedMetric(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueuePriceQueuedMetric(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMinQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMaxQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewMedianQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewCountQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "cpu"),
				commonmetrics.NewQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMinQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMaxQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewMedianQueueResources(gb, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
				commonmetrics.NewCountQueueResources(1, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue, "memory"),
			},
		},
		"running metrics": {
			initialJobs: runningJobs,
			queues:      []*api.Queue{queue},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueSizeMetric(0.0, testfixtures.TestQueue),
				commonmetrics.NewQueueDistinctSchedulingKeyMetric(0.0, testfixtures.TestQueue),
				commonmetrics.NewJobRunRunDuration(3, 300,
					map[float64]uint64{60: 1, 600: 3, 1800: 3, 3600: 3, 10800: 3, 43200: 3, 86400: 3, 172800: 3, 604800: 3},
					testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinJobRunDuration(0, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxJobRunDuration(200, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianJobRunDuration(100, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMinQueuePriceRunningMetric(100, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMaxQueuePriceRunningMetric(102, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
				commonmetrics.NewMedianQueuePriceRunningMetric(101, testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, testfixtures.TestQueue),
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
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			// set up job db with initial jobs
			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err := txn.Upsert(tc.initialJobs)
			require.NoError(t, err)
			txn.Commit()

			queueCache := schedulermocks.NewMockQueueCache(ctrl)
			queueCache.EXPECT().GetAll(ctx).Return(tc.queues, nil).Times(1)

			executorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepository.EXPECT().GetExecutors(ctx).Return([]*schedulerobjects.Executor{}, nil)
			executorRepository.EXPECT().GetExecutorSettings(ctx).Return([]*schedulerobjects.ExecutorSettings{}, nil)

			collector := NewMetricsCollector(
				jobDb,
				queueCache,
				executorRepository,
				testfixtures.TestSchedulingConfig().Pools,
				2*time.Second,
				testfixtures.TestEmptyFloatingResources,
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
				if !assert.Equal(t, e1, a1) {
					fmt.Println("here")
				}
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
	nodeWithJobs.StateByJobRunId[job1.LatestRun().Id()] = schedulerobjects.JobRunState_PENDING
	nodeWithJobs.StateByJobRunId[job2.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING
	nodeWithJobs.ResourceUsageByQueueAndPool = []*schedulerobjects.PoolQueueResource{
		{
			Pool:  testfixtures.TestPool,
			Queue: testfixtures.TestQueue,
			Resources: &schedulerobjects.ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("1"),
					"memory": pointer.MustParseResource("1Gi"),
				},
			},
		},
	}

	executorWithJobs := createExecutor("cluster-1", nodeWithJobs)

	tests := map[string]struct {
		jobDbJobs                []*jobdb.Job
		floatingResourceTypes    *floatingresources.FloatingResourceTypes
		executors                []*schedulerobjects.Executor
		expected                 []prometheus.Metric
		expectedExecutorSettings []*schedulerobjects.ExecutorSettings
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
				commonmetrics.NewClusterFarmCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterCordonedStatus(0.0, "cluster-1", "", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
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
				commonmetrics.NewClusterFarmCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterFarmCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-2"),
				commonmetrics.NewClusterFarmCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-2"),
				commonmetrics.NewClusterCordonedStatus(0.0, "cluster-1", "", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
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
				commonmetrics.NewClusterFarmCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterCordonedStatus(0.0, "cluster-1", "", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
		},
		"cluster with jobs": {
			jobDbJobs: []*jobdb.Job{job1, job2},
			executors: []*schedulerobjects.Executor{executorWithJobs},
			expected: []prometheus.Metric{
				commonmetrics.NewQueueLeasedPodCount(1, "cluster-1", testfixtures.TestPool, testfixtures.TestQueue, "Pending", "type-1"),
				commonmetrics.NewQueueLeasedPodCount(1, "cluster-1", testfixtures.TestPool, testfixtures.TestQueue, "Running", "type-1"),
				commonmetrics.NewQueueAllocated(2, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, "cpu", "type-1"),
				commonmetrics.NewQueueAllocated(2*1024*1024*1024, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, testfixtures.TestDefaultPriorityClass, "memory", "type-1"),
				commonmetrics.NewQueueUsed(1, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewQueueUsed(1*1024*1024*1024, testfixtures.TestQueue, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(1, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterFarmCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterCordonedStatus(0.0, "cluster-1", "", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
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
				commonmetrics.NewClusterFarmCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(256*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterCordonedStatus(0.0, "cluster-1", "", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
		},
		"floating resources": {
			jobDbJobs:             []*jobdb.Job{},
			floatingResourceTypes: testfixtures.TestFloatingResources,
			executors:             []*schedulerobjects.Executor{},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(10, "floating", "pool", "test-floating-resource", ""),
				commonmetrics.NewClusterTotalCapacity(10, "floating", "pool", "test-floating-resource", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{},
		},
		"cordoned cluster single node type": {
			jobDbJobs: []*jobdb.Job{},
			executors: []*schedulerobjects.Executor{executor},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(0.0, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(0.0, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(0.0, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterTotalCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterTotalCapacity(2, "cluster-1", testfixtures.TestPool, "nodes", "type-1"),
				commonmetrics.NewClusterFarmCapacity(64, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterFarmCapacity(512*1024*1024*1024, "cluster-1", testfixtures.TestPool, "memory", "type-1"),
				commonmetrics.NewClusterCordonedStatus(1.0, "cluster-1", "bad executor", ""),
			},
			expectedExecutorSettings: []*schedulerobjects.ExecutorSettings{
				{
					ExecutorId:   "cluster-1",
					Cordoned:     true,
					CordonReason: "bad executor",
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			testClock := clock.NewFakeClock(testfixtures.BaseTime)
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			// set up job db with initial jobs
			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err := txn.Upsert(tc.jobDbJobs)
			require.NoError(t, err)
			txn.Commit()

			queueCache := schedulermocks.NewMockQueueCache(ctrl)
			queueCache.EXPECT().GetAll(ctx).Return([]*api.Queue{}, nil).Times(1)

			executorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepository.EXPECT().GetExecutors(ctx).Return(tc.executors, nil)
			executorRepository.EXPECT().GetExecutorSettings(ctx).Return(tc.expectedExecutorSettings, nil)

			if tc.floatingResourceTypes == nil {
				tc.floatingResourceTypes = testfixtures.TestEmptyFloatingResources
			}

			collector := NewMetricsCollector(
				jobDb,
				queueCache,
				executorRepository,
				testfixtures.TestSchedulingConfig().Pools,
				2*time.Second,
				tc.floatingResourceTypes,
			)
			collector.clock = testClock
			err = collector.refresh(ctx)
			require.NoError(t, err)
			metricChan := make(chan prometheus.Metric, 1000) // large buffer so we don't block
			collector.Collect(metricChan)
			close(metricChan)
			actual := make([]prometheus.Metric, 0)
			for m := range metricChan {
				println(m.Desc().String())
				actual = append(actual, m)
			}
			require.NoError(t, err)
			require.Equal(t, len(tc.expected), len(actual))
			for i := 0; i < len(tc.expected); i++ {
				a1 := actual[i]
				// As resources are a map, the ordering isn't deterministic, so we have to use compare
				// Alternatively if we can work out how to sort prometheus.Metric we could do that instead
				assert.Contains(t, tc.expected, a1)
			}
		})
	}
}

func TestMetricsCollector_TestCollect_ClusterMetricsAvailableCapacity(t *testing.T) {
	node := createNode("type-1")
	job := testfixtures.TestRunningJobDbJob(0)
	node.StateByJobRunId[job.LatestRun().Id()] = schedulerobjects.JobRunState_RUNNING

	tests := map[string]struct {
		poolConfig       []configuration.PoolConfig
		runningJobs      []*jobdb.Job
		nodes            []*schedulerobjects.Node
		executorSettings []*schedulerobjects.ExecutorSettings
		expected         []prometheus.Metric
	}{
		"No away pools": {
			poolConfig: []configuration.PoolConfig{
				{Name: testfixtures.TestPool},
			},
			runningJobs:      []*jobdb.Job{job},
			nodes:            []*schedulerobjects.Node{node},
			executorSettings: []*schedulerobjects.ExecutorSettings{},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
			},
		},
		"Away pools": {
			poolConfig: []configuration.PoolConfig{
				{
					Name: testfixtures.TestPool,
				},
				{
					Name:      testfixtures.TestPool2,
					AwayPools: []string{testfixtures.TestPool},
				},
			},
			runningJobs:      []*jobdb.Job{job},
			nodes:            []*schedulerobjects.Node{node},
			executorSettings: []*schedulerobjects.ExecutorSettings{},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterAvailableCapacity(31, "cluster-1", testfixtures.TestPool2, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(31, "cluster-1", testfixtures.TestPool2, "cpu", "type-1"),
			},
		},
		"Cordoned cluster": {
			poolConfig: []configuration.PoolConfig{
				{Name: testfixtures.TestPool},
			},
			runningJobs: []*jobdb.Job{job},
			nodes:       []*schedulerobjects.Node{node},
			executorSettings: []*schedulerobjects.ExecutorSettings{
				{
					ExecutorId:   "cluster-1",
					Cordoned:     true,
					CordonReason: "bad executor",
				},
			},
			expected: []prometheus.Metric{
				commonmetrics.NewClusterAvailableCapacity(0, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
				commonmetrics.NewClusterTotalCapacity(32, "cluster-1", testfixtures.TestPool, "cpu", "type-1"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			testClock := clock.NewFakeClock(testfixtures.BaseTime)
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			// set up job db with initial jobs
			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err := txn.Upsert(tc.runningJobs)
			require.NoError(t, err)
			txn.Commit()

			queueCache := schedulermocks.NewMockQueueCache(ctrl)
			queueCache.EXPECT().GetAll(ctx).Return([]*api.Queue{}, nil).Times(1)

			executors := []*schedulerobjects.Executor{
				createExecutor("cluster-1", tc.nodes...),
			}

			executorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			executorRepository.EXPECT().GetExecutors(ctx).Return(executors, nil)
			executorRepository.EXPECT().GetExecutorSettings(ctx).Return(tc.executorSettings, nil)

			collector := NewMetricsCollector(
				jobDb,
				queueCache,
				executorRepository,
				tc.poolConfig,
				2*time.Second,
				testfixtures.TestEmptyFloatingResources,
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
				// As resources are a map, the ordering isn't deterministic, so we have to use compare
				// Alternatively if we can work out how to sort prometheus.Metric we could do that instead
				assert.Contains(t, actual, tc.expected[i])
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
	node := testfixtures.TestSchedulerObjectsNode(
		[]int32{},
		map[string]*resource.Quantity{
			"cpu":    pointer.MustParseResource("32"),
			"memory": pointer.MustParseResource("256Gi"),
		},
	)
	node.ReportingNodeType = nodeType
	node.StateByJobRunId = map[string]schedulerobjects.JobRunState{}
	node.ResourceUsageByQueueAndPool = []*schedulerobjects.PoolQueueResource{}
	return node
}
