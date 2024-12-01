package scheduling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestMarketDrivenPreemptingQueueScheduler(t *testing.T) {
	type SchedulingRound struct {
		// Map from queue name to pod requirements for that queue.
		JobsByQueue map[string][]*jobdb.Job
		// For each queue, indices of jobs expected to be scheduled.
		ExpectedScheduledIndices map[string][]int
		// For each queue, indices of jobs expected to be preempted.
		// E.g., ExpectedPreemptedIndices["A"][0] is the indices of jobs declared for queue A in round 0.
		ExpectedPreemptedIndices map[string]map[int][]int
		// For each queue, indices of jobs to unbind before scheduling, to, simulate jobs terminating.
		// E.g., IndicesToUnbind["A"][0] is the indices of jobs declared for queue A in round 0.
		IndicesToUnbind map[string]map[int][]int
		// Indices of nodes that should be cordoned before scheduling.
		NodeIndicesToCordon []int
	}
	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		// Nodes to be considered by the scheduler.
		Nodes []*schedulerobjects.Node
		// Each item corresponds to a call to Reschedule().
		Rounds []SchedulingRound
		// Map from queue to the priority factor associated with that queue.
		PriorityFactorByQueue map[string]float64
		// Map of nodeId to jobs running on those nodes
		InitialRunningJobs map[int][]*jobdb.Job
	}{
		"three users, highest price jobs from single queue get on": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
						"B": testfixtures.N1Cpu4GiJobsWithPrice("B", 101.0, 32),
						"C": testfixtures.N1Cpu4GiJobsWithPrice("C", 99.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
				},
				{
					// The system should be in steady-state; nothing should be scheduled/preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
						"C": testfixtures.N1Cpu4GiJobsWithPrice("C", 99.0, 32),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1, "C": 1},
		},
		"three users, highest price jobs between queues get on": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 11),
							testfixtures.N1Cpu4GiJobsWithPrice("A", 99.0, 21)...,
						),
						"B": append(
							testfixtures.N1Cpu4GiJobsWithPrice("B", 100.0, 11),
							testfixtures.N1Cpu4GiJobsWithPrice("B", 99.0, 21)...,
						),
						"C": append(
							testfixtures.N1Cpu4GiJobsWithPrice("C", 100.0, 11),
							testfixtures.N1Cpu4GiJobsWithPrice("C", 99.0, 21)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 10),
						"B": testfixtures.IntRange(0, 10),
						"C": testfixtures.IntRange(0, 9),
					},
				},
				{
					// The system should be in steady-state; nothing should be scheduled/preempted.
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 99.0, 21),
						"B": testfixtures.N1Cpu4GiJobsWithPrice("B", 99.0, 21),
						"C": testfixtures.N1Cpu4GiJobsWithPrice("C", 99.0, 21),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1, "C": 1},
		},
		"Two users, no preemption if price lower": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobsWithPrice("B", 99.0, 32),
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		},
		"Two users, preemption if price higher": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": testfixtures.N1Cpu4GiJobsWithPrice("B", 101.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(0, 31),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		},
		"Two users, partial preemption if price higher": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"B": append(
							testfixtures.N1Cpu4GiJobsWithPrice("B", 99.0, 16),
							testfixtures.N1Cpu4GiJobsWithPrice("B", 101.0, 16)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"B": testfixtures.IntRange(16, 31),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(16, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		},
		"Self Preemption If Price Is Higher": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": append(
							testfixtures.N1Cpu4GiJobsWithPrice("A", 99.0, 16),
							testfixtures.N1Cpu4GiJobsWithPrice("A", 101.0, 16)...,
						),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(16, 31),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(16, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1},
		},
		"Two Users. Self preemption plus cross user preemption": {
			SchedulingConfig: testfixtures.TestSchedulingConfig(),
			Nodes:            testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			Rounds: []SchedulingRound{
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 100.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 31),
					},
				},
				{
					JobsByQueue: map[string][]*jobdb.Job{
						"A": testfixtures.N1Cpu4GiJobsWithPrice("A", 102.0, 16),
						"B": testfixtures.N1Cpu4GiJobsWithPrice("B", 101.0, 32),
					},
					ExpectedScheduledIndices: map[string][]int{
						"A": testfixtures.IntRange(0, 15),
						"B": testfixtures.IntRange(0, 15),
					},
					ExpectedPreemptedIndices: map[string]map[int][]int{
						"A": {
							0: testfixtures.IntRange(0, 31),
						},
					},
				},
			},
			PriorityFactorByQueue: map[string]float64{"A": 1, "B": 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			priorities := types.AllowedPriorities(tc.SchedulingConfig.PriorityClasses)

			jobDb := jobdb.NewJobDb(tc.SchedulingConfig.PriorityClasses, tc.SchedulingConfig.DefaultPriorityClassName, stringinterner.New(1024), testfixtures.TestResourceListFactory)
			jobDbTxn := jobDb.WriteTxn()

			// Add all the initial jobs, creating runs for them
			for nodeIdx, jobs := range tc.InitialRunningJobs {
				node := tc.Nodes[nodeIdx]
				for _, job := range jobs {
					err := jobDbTxn.Upsert([]*jobdb.Job{
						job.WithQueued(false).
							WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority),
					})
					require.NoError(t, err)
				}
			}

			// Accounting across scheduling rounds.
			roundByJobId := make(map[string]int)
			indexByJobId := make(map[string]int)
			allocatedByQueueAndPriorityClass := make(map[string]map[string]internaltypes.ResourceList)
			nodeIdByJobId := make(map[string]string)
			var jobIdsByGangId map[string]map[string]bool
			var gangIdByJobId map[string]string

			// Scheduling rate-limiters persist between rounds.
			// We control the rate at which time passes between scheduling rounds.
			schedulingStarted := time.Now()
			schedulingInterval := time.Second
			limiter := rate.NewLimiter(
				rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
				tc.SchedulingConfig.MaximumSchedulingBurst,
			)
			limiterByQueue := make(map[string]*rate.Limiter)
			for queue := range tc.PriorityFactorByQueue {
				limiterByQueue[queue] = rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
					tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
				)
			}

			demandByQueue := map[string]internaltypes.ResourceList{}

			// Run the scheduler.
			cordonedNodes := map[int]bool{}
			ctx := armadacontext.Background()
			for i, round := range tc.Rounds {
				ctx.FieldLogger = ctx.WithField("round", i)
				ctx.Infof("starting scheduling round %d", i)

				jobsByNode := map[string][]*jobdb.Job{}
				for _, job := range jobDbTxn.GetAll() {
					if job.LatestRun() != nil && !job.LatestRun().InTerminalState() {
						node := job.LatestRun().NodeId()
						jobsByNode[node] = append(jobsByNode[node], job)
					}
				}

				nodeDb, err := NewNodeDb(tc.SchedulingConfig, stringinterner.New(1024))
				require.NoError(t, err)
				nodeDbTxn := nodeDb.Txn(true)
				for _, node := range tc.Nodes {
					dbNode, err := testfixtures.TestNodeFactory.FromSchedulerObjectsNode(node)
					require.NoError(t, err)
					err = nodeDb.CreateAndInsertWithJobDbJobsWithTxn(nodeDbTxn, jobsByNode[node.Name], dbNode)
					require.NoError(t, err)
				}
				nodeDbTxn.Commit()

				// Enqueue jobs that should be considered in this round.
				var queuedJobs []*jobdb.Job
				for queue, jobs := range round.JobsByQueue {
					for j, job := range jobs {
						job = job.WithQueued(true)
						require.Equal(t, queue, job.Queue())
						queuedJobs = append(queuedJobs, job.WithQueued(true))
						roundByJobId[job.Id()] = i
						indexByJobId[job.Id()] = j
						demandByQueue[job.Queue()] = demandByQueue[job.Queue()].Add(job.AllResourceRequirements())
					}
				}
				err = jobDbTxn.Upsert(queuedJobs)
				require.NoError(t, err)

				// Unbind jobs from nodes, to simulate those jobs terminating between rounds.
				for queue, reqIndicesByRoundIndex := range round.IndicesToUnbind {
					for roundIndex, reqIndices := range reqIndicesByRoundIndex {
						for _, reqIndex := range reqIndices {
							job := tc.Rounds[roundIndex].JobsByQueue[queue][reqIndex]
							nodeId := nodeIdByJobId[job.Id()]
							node, err := nodeDb.GetNode(nodeId)
							require.NoError(t, err)
							node, err = nodeDb.UnbindJobFromNode(job, node)
							require.NoError(t, err)
							err = nodeDb.Upsert(node)
							require.NoError(t, err)
							if gangId, ok := gangIdByJobId[job.Id()]; ok {
								delete(gangIdByJobId, job.Id())
								delete(jobIdsByGangId[gangId], job.Id())
							}
							demandByQueue[job.Queue()] = demandByQueue[job.Queue()].Subtract(job.AllResourceRequirements())
						}
					}
				}

				// Cordon nodes.
				for _, idx := range round.NodeIndicesToCordon {
					cordonedNodes[idx] = true
				}
				for idx, isCordoned := range cordonedNodes {
					if isCordoned {
						node, err := nodeDb.GetNode(tc.Nodes[idx].Id)
						require.NoError(t, err)
						ctx.Infof("Cordoned node %s", node.GetId())
						taints := append(slices.Clone(node.GetTaints()), internaltypes.UnschedulableTaint())
						node = testNodeWithTaints(node, taints)
						err = nodeDb.Upsert(node)
						require.NoError(t, err)
					}
				}

				// If not provided, set total resources equal to the aggregate over tc.Nodes.
				totalResources := nodeDb.TotalKubernetesResources()

				fairnessCostProvider, err := fairness.NewDominantResourceFairness(
					nodeDb.TotalKubernetesResources(),
					tc.SchedulingConfig,
				)
				require.NoError(t, err)
				sctx := context.NewSchedulingContext(
					testfixtures.TestPool,
					fairnessCostProvider,
					limiter,
					totalResources,
				)
				sctx.Started = schedulingStarted.Add(time.Duration(i) * schedulingInterval)

				for queue, priorityFactor := range tc.PriorityFactorByQueue {
					weight := 1 / priorityFactor
					queueDemand := demandByQueue[queue]
					err := sctx.AddQueueSchedulingContext(
						queue,
						weight,
						allocatedByQueueAndPriorityClass[queue],
						queueDemand,
						queueDemand,
						limiterByQueue[queue],
					)
					require.NoError(t, err)
				}
				constraints := schedulerconstraints.NewSchedulingConstraints(
					"pool",
					totalResources,
					tc.SchedulingConfig,
					armadaslices.Map(
						maps.Keys(tc.PriorityFactorByQueue),
						func(qn string) *api.Queue { return &api.Queue{Name: qn} },
					))
				sctx.UpdateFairShares()
				sch := NewPreemptingQueueScheduler(
					sctx,
					constraints,
					testfixtures.TestEmptyFloatingResources,
					true,
					tc.SchedulingConfig.ProtectedFractionOfFairShare,
					tc.SchedulingConfig.MaxQueueLookback,
					jobDbTxn,
					nodeDb,
					nodeIdByJobId,
					jobIdsByGangId,
					gangIdByJobId,
					true,
				)

				result, err := sch.Schedule(ctx)
				require.NoError(t, err)
				jobIdsByGangId = sch.jobIdsByGangId
				gangIdByJobId = sch.gangIdByJobId

				// Test resource accounting.
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					m := allocatedByQueueAndPriorityClass[job.Queue()]
					if m == nil {
						m = make(map[string]internaltypes.ResourceList)
						allocatedByQueueAndPriorityClass[job.Queue()] = m
					}
					m[job.PriorityClassName()] = m[job.PriorityClassName()].Subtract(job.AllResourceRequirements())
				}
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					m := allocatedByQueueAndPriorityClass[job.Queue()]
					if m == nil {
						m = make(map[string]internaltypes.ResourceList)
						allocatedByQueueAndPriorityClass[job.Queue()] = m
					}
					m[job.PriorityClassName()] = m[job.PriorityClassName()].Add(job.AllResourceRequirements())
				}
				for queue, qctx := range sctx.QueueSchedulingContexts {
					m := allocatedByQueueAndPriorityClass[queue]
					assert.Equal(t, internaltypes.RlMapRemoveZeros(m), internaltypes.RlMapRemoveZeros(qctx.AllocatedByPriorityClass))
				}

				// Test that jobs are mapped to nodes correctly.
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					nodeId, ok := result.NodeIdByJobId[job.Id()]
					assert.True(t, ok)
					assert.NotEmpty(t, nodeId)

					// Check that preempted jobs are preempted from the node they were previously scheduled onto.
					expectedNodeId := nodeIdByJobId[job.Id()]
					assert.Equal(t, expectedNodeId, nodeId, "job %s preempted from unexpected node", job.Id())
				}
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					nodeId, ok := result.NodeIdByJobId[job.Id()]
					assert.True(t, ok)
					assert.NotEmpty(t, nodeId)

					node, err := nodeDb.GetNode(nodeId)
					require.NoError(t, err)
					assert.NotEmpty(t, node)

					// Check that the job can actually go onto this node.
					matches, reason, err := nodedb.StaticJobRequirementsMet(node, jctx)
					require.NoError(t, err)
					assert.Empty(t, reason)
					assert.True(t, matches)

					// Check that scheduled jobs are consistently assigned to the same node.
					// (We don't allow moving jobs between nodes.)
					if expectedNodeId, ok := nodeIdByJobId[job.Id()]; ok {
						assert.Equal(t, expectedNodeId, nodeId, "job %s scheduled onto unexpected node", job.Id())
					} else {
						nodeIdByJobId[job.Id()] = nodeId
					}
				}
				for jobId, nodeId := range result.NodeIdByJobId {
					if expectedNodeId, ok := nodeIdByJobId[jobId]; ok {
						assert.Equal(t, expectedNodeId, nodeId, "job %s preempted from/scheduled onto unexpected node", jobId)
					}
				}

				// Expected scheduled jobs.
				jobIdsByQueue := jobIdsByQueueFromJobContexts(result.ScheduledJobs)
				scheduledQueues := armadamaps.MapValues(round.ExpectedScheduledIndices, func(v []int) bool { return true })
				maps.Copy(scheduledQueues, armadamaps.MapValues(jobIdsByQueue, func(v []string) bool { return true }))
				for queue := range scheduledQueues {
					expected := round.ExpectedScheduledIndices[queue]
					jobIds := jobIdsByQueue[queue]
					actual := make([]int, 0)
					for _, jobId := range jobIds {
						actual = append(actual, indexByJobId[jobId])
					}
					slices.Sort(actual)
					slices.Sort(expected)
					assert.Equal(t, expected, actual, "scheduling from queue %s", queue)
				}

				// Expected preempted jobs.
				jobIdsByQueue = jobIdsByQueueFromJobContexts(result.PreemptedJobs)
				preemptedQueues := armadamaps.MapValues(round.ExpectedPreemptedIndices, func(v map[int][]int) bool { return true })
				maps.Copy(preemptedQueues, armadamaps.MapValues(jobIdsByQueue, func(v []string) bool { return true }))
				for queue := range preemptedQueues {
					expected := round.ExpectedPreemptedIndices[queue]
					jobIds := jobIdsByQueue[queue]
					actual := make(map[int][]int)
					for _, jobId := range jobIds {
						i := roundByJobId[jobId]
						j := indexByJobId[jobId]
						actual[i] = append(actual[i], j)
					}
					for _, s := range expected {
						slices.Sort(s)
					}
					for _, s := range actual {
						slices.Sort(s)
					}
					assert.Equal(t, expected, actual, "preempting from queue %s", queue)
				}

				// We expect there to be no oversubscribed nodes.
				it, err := nodedb.NewNodesIterator(nodeDb.Txn(false))
				require.NoError(t, err)
				for node := it.NextNode(); node != nil; node = it.NextNode() {
					for _, p := range priorities {
						for _, r := range node.AllocatableByPriority[p].GetResources() {
							assert.True(t, r.RawValue >= 0, "resource %s oversubscribed by %d on node %s", r.Name, r.RawValue, node.GetId())
						}
					}
				}

				err = jobDbTxn.BatchDelete(armadaslices.Map(queuedJobs, func(job *jobdb.Job) string { return job.Id() }))
				require.NoError(t, err)

				var preemptedJobs []*jobdb.Job
				for _, jctx := range result.PreemptedJobs {
					job := jctx.Job
					preemptedJobs = append(
						preemptedJobs,
						job.
							WithUpdatedRun(job.LatestRun().WithFailed(true)).
							WithQueued(false).
							WithFailed(true),
					)
				}
				err = jobDbTxn.Upsert(preemptedJobs)
				require.NoError(t, err)

				// Jobs may arrive out of order here; sort them, so that runs
				// are created in the right order (this influences the order in
				// which jobs are preempted).
				slices.SortFunc(
					result.ScheduledJobs,
					func(a, b *context.JobSchedulingContext) int {
						if a.Job.SubmitTime().Before(b.Job.SubmitTime()) {
							return -1
						} else if b.Job.SubmitTime().Before(a.Job.SubmitTime()) {
							return 1
						} else {
							return 0
						}
					},
				)
				var scheduledJobs []*jobdb.Job
				for _, jctx := range result.ScheduledJobs {
					job := jctx.Job
					jobId := job.Id()
					node, err := nodeDb.GetNode(result.NodeIdByJobId[jobId])
					require.NotNil(t, node)
					require.NoError(t, err)
					priority, ok := nodeDb.GetScheduledAtPriority(jobId)
					require.True(t, ok)
					scheduledJobs = append(
						scheduledJobs,
						job.WithQueuedVersion(job.QueuedVersion()+1).
							WithQueued(false).
							WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), priority),
					)
				}
				err = jobDbTxn.Upsert(scheduledJobs)
				require.NoError(t, err)
			}
		})
	}
}
