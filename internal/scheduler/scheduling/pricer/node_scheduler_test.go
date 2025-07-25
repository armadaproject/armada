package pricer

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/set"

	"github.com/armadaproject/armada/internal/common/pointer"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

var (
	queueA = testfixtures.SingleQueueWithPriorityFactor("A", 10)
	queueB = testfixtures.SingleQueueWithPriorityFactor("B", 10)
	queueC = testfixtures.SingleQueueWithPriorityFactor("C", 10)
	queueD = testfixtures.SingleQueueWithPriorityFactor("D", 5)
)

func TestSchedule_NodeChecks(t *testing.T) {
	job := testfixtures.Test1Cpu16GiJob("A", testfixtures.PriorityClass1)
	jobWithNodeSelector := testfixtures.WithNodeSelectorJob(map[string]string{"armadaproject.io/nodeType": "special"}, job)
	tests := map[string]struct {
		node          *internaltypes.Node
		job           *jobdb.Job
		expectSuccess bool
	}{
		"job matches node": {
			node:          testfixtures.Test32CpuNode(testfixtures.TestPriorities),
			job:           job,
			expectSuccess: true,
		},
		"node has untolerated taints": {
			node:          testfixtures.TestTainted32CpuNode(testfixtures.TestPriorities),
			job:           job,
			expectSuccess: false,
		},
		"node doesn't match selector": {
			node:          testfixtures.TestTainted32CpuNode(testfixtures.TestPriorities),
			job:           jobWithNodeSelector,
			expectSuccess: false,
		},
		"node too small": {
			node: testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("1"),
				"memory": pointer.MustParseResource("5Gi"),
			}),
			job:           job,
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			job := testfixtures.Test1Cpu16GiJob("A", testfixtures.PriorityClass1)
			jctx := context.JobSchedulingContextFromJob(job)
			jobDb := testfixtures.JobDb
			nodeScheduler := NewMinPriceNodeScheduler(jobDb.ReadTxn())

			result, err := nodeScheduler.Schedule(jctx, tc.node, set.New(jctx.JobId))
			assert.NoError(t, err)

			assert.Equal(t, tc.expectSuccess, result.scheduled)
			assert.Equal(t, float64(0), result.price)
			assert.Empty(t, result.jobIdsToPreempt)
			assert.Equal(t, result.node, tc.node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func TestSchedule_JobChecks(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{
		"cpu":    pointer.MustParseResource("10"),
		"memory": pointer.MustParseResource("25Gi"),
	})
	higherBid := 1.0
	jobToSchedule := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	})
	jobToScheduleWithImpossibleNodeRequirements, err := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	}).WithJobSchedulingInfo(&internaltypes.JobSchedulingInfo{
		PriorityClass: testfixtures.PriorityClass0,
		SubmitTime:    time.Time{},
		PodRequirements: &internaltypes.PodRequirements{
			NodeSelector: map[string]string{
				"armadaproject.io/nodeType": "special",
			},
		},
	})
	assert.NoError(t, err)

	jobToScheduleWithImpossibleResourceRequirements := testfixtures.TestJobWithResources("A", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("20"),
		"memory": resource.MustParse("30Gi"),
	})
	existingJobBid := 0.5
	existingJob := testfixtures.TestJobWithResources("B", testfixtures.PriorityClass2, v1.ResourceList{
		"cpu":    resource.MustParse("8"),
		"memory": resource.MustParse("16Gi"),
	}).WithBidPrices(map[string]pricing.Bid{
		testfixtures.TestPool: {
			QueuedBid:  existingJobBid,
			RunningBid: existingJobBid,
		},
	})

	tests := map[string]struct {
		existingJob   *jobdb.Job
		jobToSchedule *jobdb.Job
		expectedPrice float64
		expectSuccess bool
		expectError   bool
	}{
		"single job preemption costs existing job price": {
			existingJob:   existingJob.DeepCopy().WithQueued(true),
			jobToSchedule: jobToSchedule.DeepCopy(),
			expectedPrice: existingJobBid,
			expectSuccess: true,
		},

		"single job preemption costs existing job price 2": {
			existingJob: existingJob.DeepCopy().WithQueued(true).WithBidPrices(map[string]pricing.Bid{
				testfixtures.TestPool: {
					QueuedBid:  higherBid,
					RunningBid: higherBid,
				},
			}),
			jobToSchedule: jobToSchedule.DeepCopy(),
			expectedPrice: higherBid,
			expectSuccess: true,
		},
		"job with impossible node requirements returns error": {
			existingJob:   existingJob.DeepCopy().WithQueued(true),
			jobToSchedule: jobToScheduleWithImpossibleNodeRequirements.DeepCopy(),
			expectedPrice: 0.0,
			expectSuccess: false,
			expectError:   true,
		},
		"job with impossible resource requirements returns error": {
			existingJob:   existingJob.DeepCopy().WithQueued(true),
			jobToSchedule: jobToScheduleWithImpossibleResourceRequirements.DeepCopy(),
			expectedPrice: 0.0,
			expectSuccess: false,
			expectError:   true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := node.DeepCopyNilKeys()
			jctx := context.JobSchedulingContextFromJob(tc.jobToSchedule)
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			nodeDbTxn := nodeDb.Txn(true)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, []*jobdb.Job{tc.existingJob}, node.DeepCopyNilKeys())
			require.NoError(t, err)
			nodeDbTxn.Commit()
			node, err = nodeDb.GetNode(node.GetId())
			require.NoError(t, err)
			jobDb := testfixtures.NewJobDbWithJobs([]*jobdb.Job{tc.existingJob})

			nodeScheduler := NewMinPriceNodeScheduler(jobDb.ReadTxn())

			result, err := nodeScheduler.Schedule(jctx, node, set.New(jctx.JobId))
			assert.NoError(t, err)

			assert.Equal(t, tc.expectSuccess, result.scheduled)
			if tc.expectSuccess {
				assert.Equal(t, float64(tc.expectedPrice), result.price)
				assert.Equal(t, []string{tc.existingJob.Id()}, result.jobIdsToPreempt)
			} else {
				assert.Equal(t, float64(tc.expectedPrice), result.price)
				assert.Empty(t, result.jobIdsToPreempt)
			}
			assert.Equal(t, result.node, node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func markedScheduledOnNode(jobs []*jobdb.Job, node *internaltypes.Node) []*jobdb.Job {
	result := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		result = append(result, job.WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), testfixtures.TestPool, testfixtures.TestPriorityClasses[job.PriorityClassName()].Priority))
	}
	return result
}

func TestSchedule_PreemptsExpectedJobs(t *testing.T) {
	node := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{"cpu": pointer.MustParseResource("10")})
	bigNode := testfixtures.TestNode(testfixtures.TestPriorities, map[string]*resource.Quantity{"cpu": pointer.MustParseResource("18")})
	// These jobs don't need a price attached to them
	jobToSchedule := createTestCpuJob("A", 8)
	smallJobToSchedule := createTestCpuJob("A", 3)
	bigJobToSchedule := createTestCpuJob("A", 12)
	tests := map[string]struct {
		jobToSchedule              *jobdb.Job
		queues                     []*api.Queue
		node                       *internaltypes.Node
		jobsOnNode                 []*jobdb.Job
		orderedPreemptedJobIndexes []int
		expectedResult             *NodeSchedulingResult
	}{
		"preempt jobs - same price jobs, tie break on runtime": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPrice("B", 4, 1.0), createTestCpuJobWithPrice("B", 4, 1.0)},
			orderedPreemptedJobIndexes: []int{1, 0}, // B2, B1 - Will preempt index 1 first, as it is the youngest
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     1.0,
			},
		},
		"preempt jobs - multiple differently priced jobs": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPrice("B", 2, 1.5), createTestCpuJobWithPrice("B", 2, 1.2), createTestCpuJobWithPrice("C", 2, 1.3), createTestCpuJobWithPrice("C", 2, 0.8)},
			orderedPreemptedJobIndexes: []int{3, 1, 2}, // C2, B1, C1
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     1.3,
			},
		},
		"preempt jobs - multiple differently priced jobs 2": {
			jobToSchedule:              bigJobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueD},
			node:                       bigNode,
			jobsOnNode:                 armadaslices.Concatenate(createNTestCpuJobWithPrice("B", 2, 1.5, 5), createNTestCpuJobWithPrice("D", 2, 0.4, 4)),
			orderedPreemptedJobIndexes: []int{8, 7, 6, 5, 4, 3}, // D6, D5, B3, D4, D3, B2
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     1.5,
			},
		},
		"preempt jobs - multiple differently priced jobs 3": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPrice("B", 2, 0.2), createTestCpuJobWithPrice("B", 4, 0.5)},
			orderedPreemptedJobIndexes: []int{0, 1},
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     0.5,
			},
		},
		"preempt jobs - 0 price": {
			jobToSchedule:              smallJobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPrice("B", 2, 1.0), createTestCpuJobWithPrice("B", 2, 0.0), createTestCpuJobWithPrice("B", 4, 5.0)},
			orderedPreemptedJobIndexes: []int{1}, // B2
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     0.0, // Only jobs above fairshare preempted
			},
		},
		"preempt jobs - 0 price 2": {
			jobToSchedule:              jobToSchedule,
			queues:                     []*api.Queue{queueA, queueB, queueC},
			node:                       node,
			jobsOnNode:                 []*jobdb.Job{createTestCpuJobWithPriorityClassAndPrice("B", 2, 1.0, testfixtures.PriorityClass0), createTestCpuJobWithPriorityClassAndPrice("B", 2, 0.0, testfixtures.PriorityClass0)},
			orderedPreemptedJobIndexes: []int{1}, // B2
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     0.0, // Only jobs scheduled at a lower priority preempted
			},
		},
		"preempt jobs - priority class has no bearing on preemption order": {
			jobToSchedule: jobToSchedule,
			queues:        []*api.Queue{queueA, queueB, queueC},
			node:          node,
			jobsOnNode: []*jobdb.Job{
				createTestCpuJobWithPriorityClassAndPrice("B", 2, 0.1, testfixtures.PriorityClass0), createTestCpuJobWithPrice("B", 1, 0.3), createTestCpuJobWithPrice("B", 2, 0.8),
				createTestCpuJobWithPrice("C", 2, 0.5), createTestCpuJobWithPrice("C", 2, 0.45), createTestCpuJobWithPrice("C", 1, 0.15),
			},
			orderedPreemptedJobIndexes: []int{0, 5, 1, 4, 3}, // B1 (low prio), C3 (small), B2 (small), C2, C1
			expectedResult: &NodeSchedulingResult{
				scheduled: true,
				price:     0.5,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := tc.node.DeepCopyNilKeys()
			jctx := context.JobSchedulingContextFromJob(tc.jobToSchedule)
			jobsOnNode := markedScheduledOnNode(tc.jobsOnNode, node)
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			nodeDbTxn := nodeDb.Txn(true)
			err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, jobsOnNode, node.DeepCopyNilKeys())
			require.NoError(t, err)
			nodeDbTxn.Commit()
			node, err = nodeDb.GetNode(node.GetId())
			require.NoError(t, err)
			jobDb := testfixtures.NewJobDbWithJobs(jobsOnNode)
			nodeScheduler := NewMinPriceNodeScheduler(jobDb.ReadTxn())

			result, err := nodeScheduler.Schedule(jctx, node, set.New(jctx.JobId))
			assert.NoError(t, err)

			expectedPreemptedJobIds := make([]string, 0, len(tc.orderedPreemptedJobIndexes))
			for _, index := range tc.orderedPreemptedJobIndexes {
				expectedPreemptedJobIds = append(expectedPreemptedJobIds, tc.jobsOnNode[index].Id())
			}
			tc.expectedResult.jobIdsToPreempt = expectedPreemptedJobIds
			assert.Equal(t, tc.expectedResult.scheduled, result.scheduled)
			assert.Equal(t, tc.expectedResult.price, roundFloatHighPrecision(result.price))
			assert.Equal(t, tc.expectedResult.jobIdsToPreempt, result.jobIdsToPreempt)
			assert.Equal(t, result.node, node)
			assert.Equal(t, result.jctx, jctx)
			assert.NotEmpty(t, result.resultId)
		})
	}
}

func NewNodeDb(config configuration.SchedulingConfig) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		config.PriorityClasses,
		config.IndexedResources,
		config.IndexedTaints,
		config.IndexedNodeLabels,
		config.WellKnownNodeTypes,
		testfixtures.TestResourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

func createTestCpuJob(queueName string, cpu int) *jobdb.Job {
	return createTestCpuJobWithPriorityClass(queueName, cpu, testfixtures.PriorityClass2)
}

func createTestCpuJobWithPrice(queueName string, cpu int, price float64) *jobdb.Job {
	return createTestCpuJobWithPriorityClass(queueName, cpu, testfixtures.PriorityClass2).WithBidPrices(map[string]pricing.Bid{
		testfixtures.TestPool: {
			QueuedBid:  price,
			RunningBid: price,
		},
	})
}

func createTestCpuJobWithPriorityClass(queueName string, cpu int, priorityClassName string) *jobdb.Job {
	return testfixtures.TestJobWithResources(queueName, priorityClassName, v1.ResourceList{
		"cpu": resource.MustParse(strconv.Itoa(cpu)),
	})
}

func createTestCpuJobWithPriorityClassAndPrice(queueName string, cpu int, price float64, priorityClassName string) *jobdb.Job {
	return testfixtures.TestJobWithResources(queueName, priorityClassName, v1.ResourceList{
		"cpu": resource.MustParse(strconv.Itoa(cpu)),
	}).WithBidPrices(map[string]pricing.Bid{
		testfixtures.TestPool: {
			QueuedBid:  price,
			RunningBid: price,
		},
	})
}

func createNTestCpuJobWithPrice(queueName string, cpu int, price float64, count int) []*jobdb.Job {
	result := make([]*jobdb.Job, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, createTestCpuJobWithPrice(queueName, cpu, price))
	}
	return result
}
