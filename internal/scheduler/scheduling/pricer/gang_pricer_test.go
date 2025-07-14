package pricer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestMarketBasedGangScheduler_Price(t *testing.T) {
	queueBJobs := testfixtures.N1Cpu4GiJobsQueuedWithPrice("B", testfixtures.PriorityClass1, 2.0, 10)
	queueBLargeJob1 := testfixtures.Test16Cpu128GiJobQueuedWithPrice("B", testfixtures.PriorityClass1, 3.0)
	queueCJobs := testfixtures.N1Cpu4GiJobsQueuedWithPrice("C", testfixtures.PriorityClass1, 0.5, 10)

	type NodeInfo struct {
		Node *internaltypes.Node
		Jobs []*jobdb.Job
	}

	// Price on these jobs doesn't matter
	job1 := testfixtures.Test16Cpu128GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)
	job2 := testfixtures.Test16Cpu128GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)
	job3 := testfixtures.Test1Cpu4GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)
	job4 := testfixtures.Test1Cpu4GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)
	job5 := testfixtures.Test8Cpu64GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)
	job6 := testfixtures.Test8Cpu64GiJobQueuedWithPrice("A", testfixtures.PriorityClass1, 0.0)

	tests := map[string]struct {
		Nodes []*NodeInfo
		// Jobs to try scheduling as a gang
		GangConstituents            []*jobdb.Job
		ExpectedToBeScheduled       bool
		ExpectedPrice               float64
		ExpectedUnschedulableReason string
		ExpectedError               error
	}{
		"should schedule job on nodes with lowest cost": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               0.5,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule job with zero cost if capacity is available": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job3.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule gang with zero cost if capacity is available": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job3.DeepCopy(), job4.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule gang with with lowest cost even if both scheduled on the same node": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job5.DeepCopy(), job6.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               3.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule gang on nodes with lowest cost": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               2.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule gang on nodes with lowest cost 2": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               3.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should schedule gang on nodes with lowest cost 3": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       true,
			ExpectedPrice:               2.0,
			ExpectedUnschedulableReason: "",
			ExpectedError:               nil,
		},
		"should not schedule over members of the scheduling gang": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       false,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: constraints.GangDoesNotFitUnschedulableReason,
			ExpectedError:               nil,
		},

		"cannot schedule gang where no nodes have uniformity label": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, "unknown"),
			ExpectedToBeScheduled:       false,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: GangUniformityLabelIsNotIndexedUnschedulableReason,
			ExpectedError:               nil,
		},
		"cannot schedule gang where no nodes have uniformity label 2": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.PoolNameLabel),
			ExpectedToBeScheduled:       false,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: GangNoNodesWithUniformityLabelUnschedulableReason,
			ExpectedError:               nil,
		},
		"not enough nodes matching desired uniformity label": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueCJobs[0]}},
			},
			GangConstituents:            testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			ExpectedToBeScheduled:       false,
			ExpectedPrice:               0.0,
			ExpectedUnschedulableReason: constraints.GangDoesNotFitUnschedulableReason,
			ExpectedError:               nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(queueBJobs, queueCJobs))
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			jctxs := make([]*context.JobSchedulingContext, 0, len(tc.GangConstituents))
			for _, job := range tc.GangConstituents {
				jctx := context.JobSchedulingContextFromJob(job)
				jctxs = append(jctxs, jctx)
			}
			gctx := context.NewGangSchedulingContext(jctxs)

			txn := nodeDb.Txn(true)
			allRunningJobs := []*jobdb.Job{}
			for _, nodeInfo := range tc.Nodes {
				jobs := assignJobsToNode(nodeInfo.Jobs, nodeInfo.Node)
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, nodeInfo.Node.DeepCopyNilKeys())
				require.NoError(t, err)
				allRunningJobs = append(allRunningJobs, jobs...)
			}
			txn.Commit()
			jobDbTxn := jobDb.WriteTxn()
			allJobs := append(allRunningJobs, tc.GangConstituents...)
			err = jobDbTxn.Upsert(allJobs)
			require.NoError(t, err)
			jobDbTxn.Commit()

			nodeScheduler := NewMinPriceNodeScheduler(jobDbTxn)
			gangPricer := NewGangPricer(nodeScheduler, jobDbTxn, nodeDb)

			result, err := gangPricer.Price(gctx)
			assert.Equal(t, tc.ExpectedError, err)
			assert.Equal(t, tc.ExpectedPrice, result.Price)
			assert.Equal(t, tc.ExpectedUnschedulableReason, result.UnschedulableReason)
			assert.Equal(t, tc.ExpectedToBeScheduled, result.Schedulable)
		})
	}
}

func assignJobsToNode(jobs []*jobdb.Job, node *internaltypes.Node) []*jobdb.Job {
	updated := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		j := job.WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority)
		updated = append(updated, j)
	}
	return updated
}

func createTest16CpuNodeWithLabel(labelKey string, labelValue string) *internaltypes.Node {
	node := testfixtures.Test16CpuNode(testfixtures.TestPriorities)
	nodes := testfixtures.TestNodeFactory.AddLabels([]*internaltypes.Node{node}, map[string]string{labelKey: labelValue})
	return nodes[0]
}
