package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/resource"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/pricer"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestMarketDrivenIndicativePricer_Price(t *testing.T) {
	ctx := armadacontext.Background()
	queues := armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B"), testfixtures.SingleQueuePriorityOne("C"))
	totalResource := testfixtures.CpuMem("100", "20000Gi")
	queueBJobs := testfixtures.N1Cpu4GiJobsQueuedWithPrice("B", testfixtures.PriorityClass1, 2.0, 10)
	queueBLargeJob1 := testfixtures.Test16Cpu128GiJobQueuedWithPrice("B", testfixtures.PriorityClass1, 3.0)
	queueCJobs := testfixtures.N1Cpu4GiJobsQueuedWithPrice("C", testfixtures.PriorityClass1, 0.5, 10)

	type NodeInfo struct {
		Node *internaltypes.Node
		Jobs []*jobdb.Job
	}

	tc := map[string]struct {
		nodes                    []*NodeInfo
		gangsToPrice             map[string]configuration.GangDefinition
		expectedIndicativePrices IndicativeGangPricesByJobShape
		expectedError            error
	}{
		"price single job gang at zero when allocation is available": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"single-job-gang": {
					Size:              1,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("16"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"single-job-gang": {
					Schedulable:         true,
					Price:               0.0,
					UnschedulableReason: "",
				},
			},
		},
		"price multi job gang at zero when allocation is available": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         true,
					Price:               0.0,
					UnschedulableReason: "",
				},
			},
		},
		"multi job gang priced at cost to replace existing jobs": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         true,
					Price:               3.0,
					UnschedulableReason: "",
				},
			},
		},
		"pricing multiple gangs": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"single-job-gang": {
					Size:              1,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
				"single-large-job-gang": {
					Size:              1,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("17"),
					},
				},
				"two-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
				"three-job-gang": {
					Size:              3,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"single-job-gang": {
					Schedulable:         true,
					Price:               3.0,
					UnschedulableReason: "",
				},
				"single-large-job-gang": {
					Schedulable:         false,
					Price:               0.0,
					UnschedulableReason: schedulerconstraints.JobDoesNotFitUnschedulableReason,
				},
				"two-job-gang": {
					Schedulable:         true,
					Price:               3.0,
					UnschedulableReason: "",
				},
				"three-job-gang": {
					Schedulable:         false,
					Price:               0.0,
					UnschedulableReason: schedulerconstraints.GangDoesNotFitUnschedulableReason,
				},
			},
		},
		"multi job gang priced at cheapest cost to replace existing jobs across all nodes": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("8"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         true,
					Price:               0.5,
					UnschedulableReason: "",
				},
			},
		},
		"multi job gang priced at highest cost across all nodes scheduled on": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("16"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         true,
					Price:               2.0,
					UnschedulableReason: "",
				},
			},
		},
		"multi job gang priced on the cheapest cluster": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueCJobs[2]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueCJobs[3]}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              3,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("16"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         true,
					Price:               0.5,
					UnschedulableReason: "",
				},
			},
		},
		"multi job gang can't be scheduled with incorrect node uniformity label": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              2,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    "unknown-label",
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("16"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         false,
					Price:               0.0,
					UnschedulableReason: pricer.GangUniformityLabelIsNotIndexedUnschedulableReason,
				},
			},
		},
		"multi job gang can't be scheduled when there are insufficient resources": {
			nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
			},
			gangsToPrice: map[string]configuration.GangDefinition{
				"multi-job-gang": {
					Size:              4,
					PriorityClassName: testfixtures.PriorityClass1,
					NodeUniformity:    testfixtures.ClusterNameLabel,
					Resources: &resource.ComputeResources{
						"cpu": *pointer.MustParseResource("16"),
					},
				},
			},
			expectedIndicativePrices: IndicativeGangPricesByJobShape{
				"multi-job-gang": {
					Schedulable:         false,
					Price:               0.0,
					UnschedulableReason: schedulerconstraints.GangDoesNotFitUnschedulableReason,
				},
			},
		},
	}

	for name, tc := range tc {
		t.Run(name, func(t *testing.T) {
			schedulingConfig := testfixtures.TestSchedulingConfig()
			constraints := schedulerconstraints.NewSchedulingConstraints(testfixtures.PoolNameLabel, totalResource, schedulingConfig, queues)
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(queueBJobs, queueCJobs))
			nodeDb, err := newNodeDb(schedulingConfig)
			require.NoError(t, err)

			txn := nodeDb.Txn(true)
			allRunningJobs := []*jobdb.Job{}
			for _, nodeInfo := range tc.nodes {
				jobs := assignJobsToNode(nodeInfo.Jobs, nodeInfo.Node)
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, nodeInfo.Node.DeepCopyNilKeys())
				require.NoError(t, err)
				allRunningJobs = append(allRunningJobs, jobs...)
			}
			txn.Commit()
			jobDbTxn := jobDb.WriteTxn()
			err = jobDbTxn.Upsert(allRunningJobs)
			require.NoError(t, err)
			jobDbTxn.Commit()

			jobsByQueue := armadaslices.GroupByFunc(allRunningJobs, func(job *jobdb.Job) string {
				return job.Queue()
			})

			nodeScheduler := pricer.NewMinPriceNodeScheduler(jobDbTxn)
			gangPricer := pricer.NewGangPricer(nodeScheduler, jobDbTxn, nodeDb)
			indicativePricer := NewMarketDrivenIndicativePricer(jobDbTxn, gangPricer, constraints, testfixtures.TestEmptyFloatingResources)
			sctx := createSctx(t, totalResource, queues, jobsByQueue)
			ip, err := indicativePricer.Price(ctx, sctx, jobDbTxn, tc.gangsToPrice)
			assert.Equal(t, tc.expectedIndicativePrices, ip)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

func createSctx(t *testing.T, totalResource internaltypes.ResourceList, queues []*api.Queue, runningJobsByQueue map[string][]*jobdb.Job) *context.SchedulingContext {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		totalResource,
		testfixtures.TestPool,
		testfixtures.TestSchedulingConfig(),
	)
	require.NoError(t, err)

	sctx := context.NewSchedulingContext(
		testfixtures.TestPool,
		fairnessCostProvider,
		nil,
		totalResource,
	)
	for _, q := range queues {
		weight := 1.0 / float64(q.PriorityFactor)
		unlimitedDemand := testfixtures.CpuMem("10000", "100000Gi")
		err := sctx.AddQueueSchedulingContext(
			q.Name, weight, weight,
			map[string]internaltypes.ResourceList{testfixtures.PriorityClass2: sumRequestedResource(runningJobsByQueue[q.Name])},
			unlimitedDemand,
			unlimitedDemand,
			internaltypes.ResourceList{},
			nil,
		)
		require.NoError(t, err)
	}
	sctx.UpdateFairShares()
	return sctx
}

func newNodeDb(config configuration.SchedulingConfig) (*nodedb.NodeDb, error) {
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

func assignJobsToNode(jobs []*jobdb.Job, node *internaltypes.Node) []*jobdb.Job {
	updated := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		j := job.WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority)
		updated = append(updated, j)
	}
	return updated
}

func sumRequestedResource(jobs []*jobdb.Job) internaltypes.ResourceList {
	sum := testfixtures.TestResourceListFactory.MakeAllZero()
	for _, job := range jobs {
		sum = sum.Add(job.AllResourceRequirements())
	}
	return sum
}

func createTest16CpuNodeWithLabel(labelKey string, labelValue string) *internaltypes.Node {
	node := testfixtures.Test16CpuNode(testfixtures.TestPriorities)
	nodes := testfixtures.TestNodeFactory.AddLabels([]*internaltypes.Node{node}, map[string]string{labelKey: labelValue})
	return nodes[0]
}
