package optimiser

import (
	stdcontext "context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestFairnessOptimisingScheduler_Schedule(t *testing.T) {
	queues := armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B"), testfixtures.SingleQueuePriorityOne("C"))
	totalResource := testfixtures.CpuMem("100", "2000Gi")
	queueBJobs := testfixtures.N16Cpu128GiJobs("B", testfixtures.PriorityClass1, 10)
	queueCJobs := testfixtures.N16Cpu128GiJobs("C", testfixtures.PriorityClass1, 10)

	type NodeInfo struct {
		Node          *internaltypes.Node
		Jobs          []*jobdb.Job
		ScheduleCost  float64
		PreemptedJobs []*jobdb.Job
	}

	smallJob := testfixtures.Test1Cpu4GiJob("A", testfixtures.PriorityClass1)
	largeJob := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)

	tests := map[string]struct {
		Nodes []*NodeInfo
		// Job to try scheduling.
		Job                              *jobdb.Job
		MinFairnessImprovementPercentage float64
		ShouldSchedule                   bool
		ExpectedReason                   string
		ExpectedNodeIndex                int
		PreemptedJobs                    []string
	}{
		"should schedule on lowest cost candidate node - zero cost": {
			Nodes: []*NodeInfo{
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[0], queueCJobs[0]}, ScheduleCost: 0, PreemptedJobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[2]}},
			},
			Job:               smallJob,
			ShouldSchedule:    true,
			ExpectedNodeIndex: 0,
			PreemptedJobs:     []string{queueBJobs[0].Id()},
		},
		"should schedule on lowest cost candidate node": {
			Nodes: []*NodeInfo{
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[0], queueCJobs[0]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}, ScheduleCost: 0.12, PreemptedJobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}, ScheduleCost: 0.14, PreemptedJobs: []*jobdb.Job{queueBJobs[2]}},
			},
			Job:               smallJob,
			ShouldSchedule:    true,
			ExpectedNodeIndex: 0,
			PreemptedJobs:     []string{queueBJobs[0].Id()},
		},
		"should not schedule - MinFairnessImprovementPercentage": {
			ExpectedReason: constraints.OptimiserNoFairnessImprovementUnschedulableReason,
			Nodes: []*NodeInfo{
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[0], queueCJobs[0]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}, ScheduleCost: 0.12, PreemptedJobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}, ScheduleCost: 0.14, PreemptedJobs: []*jobdb.Job{queueBJobs[2]}},
			},
			Job: smallJob,
			// There is 100 total resource, so a 1 cpu job costs 0.01
			// The cost to schedule is 0.1, so this won't meet the 10% improvement threshold
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
			ExpectedNodeIndex:                0,
			PreemptedJobs:                    []string{queueBJobs[0].Id()},
		},
		"should schedule - MinFairnessImprovementPercentage": {
			Nodes: []*NodeInfo{
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[0], queueCJobs[0]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}, ScheduleCost: 0.12, PreemptedJobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}, ScheduleCost: 0.14, PreemptedJobs: []*jobdb.Job{queueBJobs[2]}},
			},
			Job: largeJob,
			// There is 100 total resource, so a 16 cpu job costs 0.16
			// The cost to schedule is 0.1, so this will meet the 10% improvement threshold (0.16/0.1 = 160%, 60% improvement)
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                0,
			PreemptedJobs:                    []string{queueBJobs[0].Id()},
		},
		"error": {
			Nodes: []*NodeInfo{
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[0], queueCJobs[0]}, ScheduleCost: 0.1, PreemptedJobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}, ScheduleCost: 0.12, PreemptedJobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}, ScheduleCost: 0.14, PreemptedJobs: []*jobdb.Job{queueBJobs[2]}},
			},
			Job: largeJob,
			// There is 100 total resource, so a 16 cpu job costs 0.16
			// The cost to schedule is 0.1, so this will meet the 10% improvement threshold (0.16/0.1 = 160%, 60% improvement)
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                0,
			PreemptedJobs:                    []string{queueBJobs[0].Id()},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			stubNodeScheduler := &StubNodeScheduler{resultByNodeId: make(map[string]*nodeSchedulingResult)}
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(queueBJobs, queueCJobs))
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			jctx := context.JobSchedulingContextFromJob(tc.Job)
			gctx := context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx})

			txn := nodeDb.Txn(true)
			allRunningJobs := []*jobdb.Job{}
			for _, nodeInfo := range tc.Nodes {
				jobs := assignJobsToNode(nodeInfo.Jobs, nodeInfo.Node)
				err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, nodeInfo.Node.DeepCopyNilKeys())
				require.NoError(t, err)
				allRunningJobs = append(allRunningJobs, jobs...)

				nodeResult := createNodeSchedulingResult(jctx, nodeInfo.Node, nodeInfo.ScheduleCost, nodeInfo.PreemptedJobs...)
				stubNodeScheduler.resultByNodeId[nodeInfo.Node.GetId()] = nodeResult
			}
			txn.Commit()
			jobDbTxn := jobDb.WriteTxn()
			allJobs := append(allRunningJobs, []*jobdb.Job{tc.Job}...)
			err = jobDbTxn.Upsert(allJobs)
			require.NoError(t, err)
			jobDbTxn.Commit()

			nodes, err := nodeDb.GetNodes()
			require.NoError(t, err)

			jobsByQueue := armadaslices.GroupByFunc(allRunningJobs, func(job *jobdb.Job) string {
				return job.Queue()
			})

			improvementThreshold := float64(-100000) // Effectively no threshold
			if tc.MinFairnessImprovementPercentage > 0 {
				improvementThreshold = tc.MinFairnessImprovementPercentage
			}
			gangScheduler := NewFairnessOptimisingScheduler(stubNodeScheduler, jobDbTxn, nodeDb, improvementThreshold, false)
			sctx := createSctx(t, totalResource, queues, jobsByQueue)

			ctx := armadacontext.Background()
			scheduled, preemptedJobs, reason, err := gangScheduler.Schedule(ctx, gctx, sctx)
			assert.NoError(t, err)
			expectedScheduledNodes := []*internaltypes.Node{tc.Nodes[tc.ExpectedNodeIndex].Node}
			if tc.ShouldSchedule {
				assert.Empty(t, reason)
				assert.True(t, scheduled)
				assert.Equal(t, len(preemptedJobs), len(tc.PreemptedJobs))
				actualPreemptedJobIds := armadaslices.Map(preemptedJobs, func(jctx *context.JobSchedulingContext) string {
					return jctx.JobId
				})
				assertStringListsEqual(t, tc.PreemptedJobs, actualPreemptedJobIds)
			} else {
				assert.Equal(t, tc.ExpectedReason, reason)
				assert.False(t, constraints.UnschedulableReasonIsPropertyOfGang(reason))
				assert.False(t, scheduled)
				assert.Empty(t, preemptedJobs)
				expectedScheduledNodes = []*internaltypes.Node{}
			}
			assertExpectedJctxUpdates(t, sctx, gctx, expectedScheduledNodes, tc.ShouldSchedule)
			assertExpectedNodeUpdates(t, gctx, preemptedJobs, nodeDb, nodes, expectedScheduledNodes)
			assertExpectedSctxUpdates(t, sctx, gctx, preemptedJobs, jobsByQueue)
		})
	}
}

func TestFairnessOptimisingScheduler_Schedule_WhenSubmittingGangs(t *testing.T) {
	queues := armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B"), testfixtures.SingleQueuePriorityOne("C"))
	totalResource := testfixtures.CpuMem("100", "20000Gi")
	queueBJobs := testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 10)
	queueBLargeJob1 := testfixtures.Test16Cpu128GiJob("B", testfixtures.PriorityClass1)
	queueCJobs := testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass1, 10)

	type NodeInfo struct {
		Node *internaltypes.Node
		Jobs []*jobdb.Job
	}

	job1 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)
	job2 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)
	gangB := testfixtures.WithGangAnnotationsJobs(testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 2))
	largeGangB := testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{
		createTestCpuJobWithPriorityClass("B", 8, testfixtures.PriorityClass1),
		createTestCpuJobWithPriorityClass("B", 8, testfixtures.PriorityClass1),
	})
	expensiveGangB := testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{
		testfixtures.Test1Cpu4GiJob("B", testfixtures.PriorityClass1),
		testfixtures.Test32Cpu256GiJob("B", testfixtures.PriorityClass1),
	})
	gangC := testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{
		createTestCpuJobWithPriorityClass("C", 2, testfixtures.PriorityClass1),
		createTestCpuJobWithPriorityClass("C", 2, testfixtures.PriorityClass1),
	})

	tests := map[string]struct {
		Nodes               []*NodeInfo
		AllowGangPreemption bool
		// Jobs to try scheduling as a gang
		Jobs                             []*jobdb.Job
		MinFairnessImprovementPercentage float64
		ShouldSchedule                   bool
		ExpectedReason                   string
		ExpectedNodeIndex                []int
		PreemptedJobs                    []string
	}{
		"preempted siblings make room for the rest of the incoming gang": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: largeGangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: largeGangB[1:]},
			},
			Jobs:                testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}),
			AllowGangPreemption: true,
			// Removing 16 CPU to place 32 CPU passes, although the first placement alone would not.
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{0, 1},
			PreemptedJobs:                    []string{largeGangB[0].Id(), largeGangB[1].Id()},
		},
		"prefer nodes that avoid preempting a sibling in another cluster": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: expensiveGangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: queueCJobs[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: queueCJobs[1:2]},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithLabels(map[string]string{testfixtures.ClusterNameLabel: "cluster2"}), Jobs: expensiveGangB[1:]},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{1, 2},
			PreemptedJobs:                    []string{queueCJobs[0].Id(), queueCJobs[1].Id()},
		},
		"reject placement when the sibling outside the node group makes it too expensive": {
			ExpectedReason: constraints.OptimiserNoFairnessImprovementUnschedulableReason,
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: expensiveGangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1")},
				{Node: testfixtures.Test32CpuNode(testfixtures.TestPriorities).WithLabels(map[string]string{testfixtures.ClusterNameLabel: "cluster2"}), Jobs: expensiveGangB[1:]},
				// This group cannot fit the whole gang; the other groups fail on fairness.
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster3")},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
		},
		"move an incoming job to keep the first preempted job on its node": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: queueCJobs[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangB[1:]},
			},
			Jobs:                             testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{1, 2},
			PreemptedJobs:                    []string{gangB[0].Id(), gangB[1].Id()},
		},
		"keep jobs in preemption order when only one can stay": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1").WithId("node-0"), Jobs: queueCJobs[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1").WithId("node-1"), Jobs: queueCJobs[1:2]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1").WithId("node-2"), Jobs: gangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1").WithId("node-3"), Jobs: gangB[1:]},
			},
			Jobs:                             testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy(), testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)}),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{1, 2, 3},
			PreemptedJobs:                    []string{queueCJobs[1].Id(), gangB[0].Id(), gangB[1].Id()},
		},
		"keeping a gang also keeps its sibling outside the node group": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangC[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangC[1:]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: gangB[1:]},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{1, 2},
			PreemptedJobs:                    []string{gangC[0].Id(), gangC[1].Id()},
		},
		"keep the original placement when a node selector prevents reducing preemption": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: queueCJobs[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2").WithId("node-1"), Jobs: gangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2").WithId("node-2"), Jobs: gangB[1:]},
			},
			Jobs: testfixtures.WithGangAnnotationsJobs([]*jobdb.Job{
				testfixtures.WithNodeSelectorJob(map[string]string{testfixtures.ClusterNameLabel: "cluster1"}, job1.DeepCopy()),
				job2.DeepCopy(),
			}),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{0, 1},
			PreemptedJobs:                    []string{queueCJobs[0].Id(), gangB[0].Id(), gangB[1].Id()},
		},
		"a failed node group leaves its jobs running when another group succeeds": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: gangB[:1]},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2")},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2")},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster3"), Jobs: gangB[1:]},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			AllowGangPreemption:              true,
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{1, 2},
			PreemptedJobs:                    []string{},
		},
		"should schedule gang on nodes with lowest cost": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{0, 1},
			PreemptedJobs:                    []string{queueBJobs[0].Id(), queueCJobs[0].Id()},
		},
		"should not schedule over members of the scheduling gang": {
			ExpectedReason: constraints.OptimiserNoPlacementUnschedulableReason,
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
		},
		"MinFairnessImprovementPercentage -  must be met by all members of the gang": {
			ExpectedReason: constraints.OptimiserNoFairnessImprovementUnschedulableReason,
			Nodes: []*NodeInfo{
				// queueBLargeJob1 takes up the full node and the queue is below fairshare, preempting it won't lead to a fairness improvement
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBLargeJob1}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
		},
		"nodeUniformityLabel - label must be indexed": {
			ExpectedReason: "uniformity label unknown is not indexed",
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, "unknown"),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
		},
		"nodeUniformityLabel - no matching nodes": {
			ExpectedReason: "no nodes with uniformity label " + testfixtures.PoolNameLabel,
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.PoolNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
		},
		"nodeUniformityLabel - not enough nodes in each group": {
			ExpectedReason: constraints.OptimiserNoPlacementUnschedulableReason,
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueCJobs[0]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   false,
		},
		"nodeUniformityLabel - not pick the group with the lowest cost": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueBJobs[1], queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster2"), Jobs: []*jobdb.Job{queueBJobs[2], queueCJobs[2]}},
			},
			Jobs:                             testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy()}, testfixtures.ClusterNameLabel),
			MinFairnessImprovementPercentage: 10,
			ShouldSchedule:                   true,
			ExpectedNodeIndex:                []int{0, 1},
			PreemptedJobs:                    []string{queueBJobs[0].Id(), queueCJobs[0].Id()},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(queueBJobs, queueCJobs))
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			jctxs := make([]*context.JobSchedulingContext, 0, len(tc.Jobs))
			for _, job := range tc.Jobs {
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
			allJobs := append(allRunningJobs, tc.Jobs...)
			err = jobDbTxn.Upsert(allJobs)
			require.NoError(t, err)
			jobDbTxn.Commit()

			nodes, err := nodeDb.GetNodes()
			require.NoError(t, err)

			jobsByQueue := armadaslices.GroupByFunc(allRunningJobs, func(job *jobdb.Job) string {
				return job.Queue()
			})

			improvementThreshold := float64(-100000) // Effectively no threshold
			if tc.MinFairnessImprovementPercentage > 0 {
				improvementThreshold = tc.MinFairnessImprovementPercentage
			}
			nodeScheduler := NewPreemptingNodeScheduler(jobDbTxn, nodeDb, nil, tc.AllowGangPreemption)
			gangScheduler := NewFairnessOptimisingScheduler(nodeScheduler, jobDbTxn, nodeDb, improvementThreshold, tc.AllowGangPreemption)
			sctx := createSctx(t, totalResource, queues, jobsByQueue)

			ctx := armadacontext.Background()
			scheduled, preemptedJobs, reason, err := gangScheduler.Schedule(ctx, gctx, sctx)
			assert.NoError(t, err)
			expectedScheduledNodes := []*internaltypes.Node{}
			for _, index := range tc.ExpectedNodeIndex {
				expectedScheduledNodes = append(expectedScheduledNodes, tc.Nodes[index].Node)
			}
			if tc.ShouldSchedule {
				assert.Empty(t, reason)
				assert.True(t, scheduled)
				assert.Equal(t, len(preemptedJobs), len(tc.PreemptedJobs))
				actualPreemptedJobIds := armadaslices.Map(preemptedJobs, func(jctx *context.JobSchedulingContext) string {
					return jctx.JobId
				})
				assertStringListsEqual(t, tc.PreemptedJobs, actualPreemptedJobIds)
			} else {
				assert.Equal(t, tc.ExpectedReason, reason)
				assert.False(t, constraints.UnschedulableReasonIsPropertyOfGang(reason))
				assert.False(t, scheduled)
				assert.Empty(t, preemptedJobs)
				expectedScheduledNodes = []*internaltypes.Node{}
			}
			assertExpectedJctxUpdates(t, sctx, gctx, expectedScheduledNodes, tc.ShouldSchedule)
			assertExpectedNodeUpdates(t, gctx, preemptedJobs, nodeDb, nodes, expectedScheduledNodes)
			assertExpectedSctxUpdates(t, sctx, gctx, preemptedJobs, jobsByQueue)
		})
	}
}

func TestFairnessOptimisingScheduler_ScheduleGangsEvenly(t *testing.T) {
	queues := armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B"), testfixtures.SingleQueuePriorityOne("C"))
	totalResource := testfixtures.CpuMem("100", "20000Gi")
	queueBJobs := testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 10)
	queueCJobs := testfixtures.N1Cpu4GiJobs("C", testfixtures.PriorityClass1, 10)

	type NodeInfo struct {
		Node *internaltypes.Node
		Jobs []*jobdb.Job
	}

	job1 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)
	job2 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)
	job3 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)
	job4 := testfixtures.Test16Cpu128GiJob("A", testfixtures.PriorityClass1)

	tests := map[string]struct {
		Nodes []*NodeInfo
		// Jobs to try scheduling as a gang
		Jobs                         []*jobdb.Job
		ExpectedPreemptedJobsByQueue map[string]int
	}{
		"should schedule gang evenly over queues": {
			Nodes: []*NodeInfo{
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueBJobs[2]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[0]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[1]}},
				{Node: createTest16CpuNodeWithLabel(testfixtures.ClusterNameLabel, "cluster1"), Jobs: []*jobdb.Job{queueCJobs[2]}},
			},
			Jobs: testfixtures.WithNodeUniformityGangAnnotationsJobs([]*jobdb.Job{job1.DeepCopy(), job2.DeepCopy(), job3.DeepCopy(), job4.DeepCopy()}, testfixtures.ClusterNameLabel),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(queueBJobs, queueCJobs))
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			jctxs := make([]*context.JobSchedulingContext, 0, len(tc.Jobs))
			for _, job := range tc.Jobs {
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
			allJobs := append(allRunningJobs, tc.Jobs...)
			err = jobDbTxn.Upsert(allJobs)
			require.NoError(t, err)
			jobDbTxn.Commit()

			jobsByQueue := armadaslices.GroupByFunc(allRunningJobs, func(job *jobdb.Job) string {
				return job.Queue()
			})

			improvementThreshold := float64(-100000) // Effectively no threshold
			nodeScheduler := NewPreemptingNodeScheduler(jobDbTxn, nodeDb, nil, false)
			gangScheduler := NewFairnessOptimisingScheduler(nodeScheduler, jobDbTxn, nodeDb, improvementThreshold, false)
			sctx := createSctx(t, totalResource, queues, jobsByQueue)

			ctx := armadacontext.Background()
			scheduled, preemptedJobs, reason, err := gangScheduler.Schedule(ctx, gctx, sctx)
			assert.NoError(t, err)
			assert.Empty(t, reason)
			assert.True(t, scheduled)

			preemptedJobsByQueue := armadaslices.GroupByFunc(preemptedJobs, func(jctx *context.JobSchedulingContext) string {
				return jctx.Job.Queue()
			})
			for queue, expectedPreemptedJobs := range tc.ExpectedPreemptedJobsByQueue {
				preemptedJobs := preemptedJobsByQueue[queue]
				assert.Len(t, preemptedJobs, expectedPreemptedJobs)
			}
		})
	}
}

func TestFairnessOptimisingScheduler_ContextExpiry(t *testing.T) {
	// B occupies 9 of the node's 16 CPUs. A needs all 16 CPUs, in one or two jobs.
	node := testfixtures.Test16CpuNode(testfixtures.TestPriorities).WithId("node-0")
	secondNode := testfixtures.Test16CpuNode(testfixtures.TestPriorities).WithId("node-1")
	runningJobs := assignJobsToNode(createNTestCpuJob("B", 9, 1), node)
	queues := armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B"))
	expired, cancelDeadline := stdcontext.WithDeadline(stdcontext.Background(), time.Unix(0, 0))
	defer cancelDeadline()
	canceled, cancel := stdcontext.WithCancel(stdcontext.Background())
	cancel()

	tests := map[string]struct {
		Jobs                  []*jobdb.Job
		InitialContext        stdcontext.Context
		ContextAfterFirstNode stdcontext.Context
		ShouldSchedule        bool
	}{
		"deadline before scheduling returns no reason": {
			Jobs:           createNTestCpuJob("A", 16, 1),
			InitialContext: expired,
		},
		"cancellation before scheduling returns no reason": {
			Jobs:           createNTestCpuJob("A", 16, 1),
			InitialContext: canceled,
		},
		"deadline before reduction doesn't schedule": {
			Jobs:                  createNTestCpuJob("A", 16, 1),
			ContextAfterFirstNode: expired,
		},
		"deadline after first member discards a partial placement": {
			Jobs:                  createNTestCpuJob("A", 8, 2),
			ContextAfterFirstNode: expired,
		},
		"cancellation discards even a complete placement": {
			Jobs:                  createNTestCpuJob("A", 16, 1),
			ContextAfterFirstNode: canceled,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobs := testfixtures.WithGangAnnotationsJobs(tc.Jobs)
			gctx := context.NewGangSchedulingContext(armadaslices.Map(jobs, context.JobSchedulingContextFromJob))
			jobDb := testfixtures.NewJobDbWithJobs(armadaslices.Concatenate(runningJobs, jobs))
			nodeDb, err := NewNodeDb(testfixtures.TestSchedulingConfig())
			require.NoError(t, err)
			txn := nodeDb.Txn(true)
			require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, runningJobs, node.DeepCopyNilKeys()))
			require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, secondNode.DeepCopyNilKeys()))
			txn.Commit()
			nodes, err := nodeDb.GetNodes()
			require.NoError(t, err)
			jobsByQueue := map[string][]*jobdb.Job{"B": runningJobs}
			sctx := createSctx(t, testfixtures.CpuMem("100", "2000Gi"), queues, jobsByQueue)
			ctx := armadacontext.Background()
			if tc.InitialContext != nil {
				ctx.Context = tc.InitialContext
			}
			nodeScheduler := &StubNodeScheduler{
				resultByNodeId: map[string]*nodeSchedulingResult{
					node.GetId(): createNodeSchedulingResult(gctx.JobSchedulingContexts[0], node, 0.09, runningJobs...),
				},
				onSchedule: func() { ctx.Context = tc.ContextAfterFirstNode },
			}
			scheduler := NewFairnessOptimisingScheduler(nodeScheduler, jobDb.ReadTxn(), nodeDb, 10, true)

			scheduled, preemptedJobs, reason, err := scheduler.Schedule(ctx, gctx, sctx)

			require.NoError(t, err)
			assert.Equal(t, tc.ShouldSchedule, scheduled)
			expectedNodes := []*internaltypes.Node{}
			if tc.ShouldSchedule {
				assert.Empty(t, reason)
				require.Len(t, preemptedJobs, 1)
				assert.Equal(t, runningJobs[0].Id(), preemptedJobs[0].JobId)
				expectedNodes = append(expectedNodes, node)
				assert.Equal(t, 1, sctx.NumScheduledGangs)
			} else {
				assert.Empty(t, reason)
				assert.Empty(t, preemptedJobs)
				assert.Empty(t, sctx.PreemptedJobIds)
				assert.Zero(t, sctx.NumScheduledGangs)
			}
			assertExpectedJctxUpdates(t, sctx, gctx, expectedNodes, tc.ShouldSchedule)
			assertExpectedNodeUpdates(t, gctx, preemptedJobs, nodeDb, nodes, expectedNodes)
			assertExpectedSctxUpdates(t, sctx, gctx, preemptedJobs, jobsByQueue)
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

func assertExpectedNodeUpdates(
	t *testing.T, gctx *context.GangSchedulingContext,
	preemptedJobs []*context.JobSchedulingContext,
	nodeDb *nodedb.NodeDb, originalNodeStates []*internaltypes.Node,
	expectedScheduledNodes []*internaltypes.Node,
) {
	preemptedJobsByNodeId := armadaslices.MapAndGroupByFuncs(preemptedJobs, func(jctx *context.JobSchedulingContext) string {
		return jctx.AssignedNode.GetId()
	}, func(jctx *context.JobSchedulingContext) string {
		return jctx.JobId
	})
	originalNodesById := armadaslices.GroupByFuncUnique(originalNodeStates, func(node *internaltypes.Node) string {
		return node.GetId()
	})
	expectedScheduledNodesById := armadaslices.GroupByFuncUnique(expectedScheduledNodes, func(node *internaltypes.Node) string {
		return node.GetId()
	})

	nodes, err := nodeDb.GetNodes()
	assert.NoError(t, err)
	for _, node := range nodes {
		assert.False(t, internaltypes.ResourceList{}.Exceeds(node.AllocatableAtPriority(internaltypes.EvictedPriority)))
		// Should have been scheduled
		if _, shouldBeScheduled := expectedScheduledNodesById[node.GetId()]; shouldBeScheduled {
			for _, jobId := range preemptedJobsByNodeId[node.GetId()] {
				assert.NotContains(t, node.GetRunningJobIds(), jobId)
			}

			jobsOnNode := armadaslices.Filter(gctx.JobSchedulingContexts, func(jctx *context.JobSchedulingContext) bool {
				return jctx.PodSchedulingContext != nil && jctx.PodSchedulingContext.NodeId == node.GetId()
			})
			assert.True(t, len(jobsOnNode) > 0)
			for _, job := range jobsOnNode {
				assert.Contains(t, node.GetRunningJobIds(), job.JobId)
			}
		} else {
			expectedJobIds := armadaslices.Subtract(originalNodesById[node.GetId()].GetRunningJobIds(), preemptedJobsByNodeId[node.GetId()])
			assertStringListsEqual(t, expectedJobIds, node.GetRunningJobIds())
		}
	}
}

func assertExpectedSctxUpdates(t *testing.T, sctx *context.SchedulingContext, gctx *context.GangSchedulingContext, preemptedJctxs []*context.JobSchedulingContext, originalJobsPerQueue map[string][]*jobdb.Job) {
	scheduledJobIds := armadaslices.Map(gctx.JobSchedulingContexts, func(jctx *context.JobSchedulingContext) string {
		return jctx.JobId
	})
	preemptedJobs := armadaslices.Map(preemptedJctxs, func(jctx *context.JobSchedulingContext) *jobdb.Job {
		return jctx.Job
	})
	preemptedJobsByQueue := armadaslices.GroupByFunc(preemptedJobs, func(job *jobdb.Job) string {
		return job.Queue()
	})

	// Assert preempted jobs marked as preempted
	for _, preemptedJctx := range preemptedJctxs {
		preemptedJob := preemptedJctx.Job
		assert.Contains(t, scheduledJobIds, preemptedJctx.GetPreemptingJob().Id())
		assert.NotEmpty(t, preemptedJctx.PreemptionDescription)
		assert.Equal(t, context.PreemptedWithOptimiserPreemption, preemptedJctx.PreemptionType)

		_, exists := sctx.QueueSchedulingContexts[preemptedJob.Queue()].SuccessfulJobSchedulingContexts[preemptedJob.Id()]
		assert.False(t, exists)
		_, exists = sctx.QueueSchedulingContexts[preemptedJob.Queue()].PreemptedByOptimiserJobSchedulingContexts[preemptedJob.Id()]
		assert.True(t, exists)
	}

	// Assert preempted queues have their allocation updated
	for queue, originalJobsPerQueue := range originalJobsPerQueue {
		originalAllocation := sumRequestedResource(originalJobsPerQueue)
		preemptedJobsForQueue := preemptedJobsByQueue[queue]
		expectedAllocation := originalAllocation.Subtract(sumRequestedResource(preemptedJobsForQueue))

		assert.Equal(t, expectedAllocation, sctx.QueueSchedulingContexts[queue].Allocated)
	}
}

func assertExpectedJctxUpdates(t *testing.T, sctx *context.SchedulingContext, gctx *context.GangSchedulingContext, expectedScheduledNodes []*internaltypes.Node, expectedToBeScheduled bool) {
	expectedScheduledNodesById := armadaslices.GroupByFuncUnique(expectedScheduledNodes, func(node *internaltypes.Node) string {
		return node.GetId()
	})

	if expectedToBeScheduled {
		assert.Equal(t, gctx.TotalResourceRequests, sctx.QueueSchedulingContexts[gctx.Queue].Allocated)
	}

	for _, jctx := range gctx.JobSchedulingContexts {
		if expectedToBeScheduled {
			_, exists := sctx.QueueSchedulingContexts[jctx.Job.Queue()].SuccessfulJobSchedulingContexts[jctx.Job.Id()]
			assert.True(t, exists)

			assert.NotNil(t, jctx.PodSchedulingContext)
			pctx := jctx.PodSchedulingContext
			assert.Contains(t, maps.Keys(expectedScheduledNodesById), pctx.NodeId)
			assert.Equal(t, context.ScheduledWithFairnessOptimiser, pctx.SchedulingMethod)
			assert.Equal(t, jctx.Job.PriorityClass().Priority, pctx.ScheduledAtPriority)
			assert.True(t, pctx.IsSuccessful())
		} else {
			assert.Empty(t, sctx.QueueSchedulingContexts[jctx.Job.Queue()].SuccessfulJobSchedulingContexts)
			assert.True(t, sctx.QueueSchedulingContexts[jctx.Job.Queue()].Allocated.AllZero())
			assert.Nil(t, jctx.PodSchedulingContext)
		}
	}
}

func assertStringListsEqual(t *testing.T, expected []string, actual []string) {
	slices.Sort(expected)
	slices.Sort(actual)
	assert.Equal(t, expected, actual)
}

func createNodeSchedulingResult(jctx *context.JobSchedulingContext, node *internaltypes.Node, cost float64, preemptedJobs ...*jobdb.Job) *nodeSchedulingResult {
	gangs := make([]*preemptibleGang, 0, len(preemptedJobs))
	for _, job := range preemptedJobs {
		preemptedJctx := context.JobSchedulingContextFromJob(job)
		preemptedJctx.AssignedNode = node
		gangs = append(gangs, &preemptibleGang{preemptedSiblingJobId: job.Id(), queue: job.Queue(), jobs: []*context.JobSchedulingContext{preemptedJctx}, totalResources: job.AllResourceRequirements()})
	}
	return &nodeSchedulingResult{
		scheduled:      true,
		jctx:           jctx,
		node:           node,
		schedulingCost: cost,
		gangsToPreempt: gangs,
	}
}

func sumRequestedResource(jobs []*jobdb.Job) internaltypes.ResourceList {
	sum := testfixtures.TestResourceListFactory.MakeAllZero()
	for _, job := range jobs {
		sum = sum.Add(job.AllResourceRequirements())
	}
	return sum
}

func assignJobsToNode(jobs []*jobdb.Job, node *internaltypes.Node) []*jobdb.Job {
	updated := make([]*jobdb.Job, 0, len(jobs))
	for _, job := range jobs {
		j := job.WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority)
		updated = append(updated, j)
	}
	return updated
}

type StubNodeScheduler struct {
	resultByNodeId map[string]*nodeSchedulingResult
	scheduleError  error
	onSchedule     func()
}

func (s *StubNodeScheduler) Schedule(schedContext *SchedulingContext,
	jctx *context.JobSchedulingContext, node *internaltypes.Node,
) (*nodeSchedulingResult, error) {
	if s.onSchedule != nil {
		s.onSchedule()
	}
	if s.scheduleError != nil {
		return nil, s.scheduleError
	}

	if result, present := s.resultByNodeId[node.GetId()]; present {
		return result, nil
	}

	return &nodeSchedulingResult{
		scheduled: false,
		jctx:      jctx,
		node:      node,
	}, nil
}

func createTest16CpuNodeWithLabel(labelKey string, labelValue string) *internaltypes.Node {
	node := testfixtures.Test16CpuNode(testfixtures.TestPriorities)
	nodes := testfixtures.TestNodeFactory.AddLabels([]*internaltypes.Node{node}, map[string]string{labelKey: labelValue})
	return nodes[0]
}
