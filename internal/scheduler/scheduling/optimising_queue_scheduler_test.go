package scheduling

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/optimiser"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

var (
	defaultOptimiserConfig = createOptimiserConfig(100000, map[string]float64{}, nil)
	gangSchedulerError     = fmt.Errorf("gang scheduler error")
)

func TestOptimisingQueueScheduler_Schedule(t *testing.T) {
	preemptedJobs := testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 2)
	preemptedJctx1 := context.JobSchedulingContextFromJob(preemptedJobs[0])
	preemptedJctx2 := context.JobSchedulingContextFromJob(preemptedJobs[1])

	alwaysSucceedGangScheduler := &StubGangScheduler{t: t, scheduleSuccess: true, scheduleError: nil, scheduleFailedReason: "", preemptedJobs: make([]*context.JobSchedulingContext, 0)}
	alwaysSucceedGangSchedulerWithPreemptedJobs := &StubGangScheduler{t: t, scheduleSuccess: true, scheduleError: nil, scheduleFailedReason: "", preemptedJobs: []*context.JobSchedulingContext{preemptedJctx1, preemptedJctx2}}
	alwaysFailsGangScheduler := &StubGangScheduler{t: t, scheduleSuccess: false, scheduleError: nil, scheduleFailedReason: schedulerconstraints.JobDoesNotFitUnschedulableReason, preemptedJobs: make([]*context.JobSchedulingContext, 0)}
	alwaysErrorGangScheduler := &StubGangScheduler{t: t, scheduleSuccess: false, scheduleError: gangSchedulerError, scheduleFailedReason: "", preemptedJobs: make([]*context.JobSchedulingContext, 0)}
	schedulerConfig := testfixtures.WithOptimiserConfig(testfixtures.TestPool, defaultOptimiserConfig, testfixtures.TestSchedulingConfig())

	tests := map[string]struct {
		SchedulingConfig configuration.SchedulingConfig
		GangScheduler    optimiser.GangScheduler
		Queues           []*api.Queue
		// Scheduling capacity
		TotalSchedulingCapacity internaltypes.ResourceList
		// Initial resource usage by queues.
		InitialAllocatedByQueue map[string]internaltypes.ResourceList
		// Jobs to try scheduling.
		Jobs []*jobdb.Job
		// Indicies of jobs that should will be marked as already successfully scheduled in the sctx
		AlreadyScheduledThisRoundIndicies []int
		// Indices of jobs expected to be scheduled.
		ExpectedScheduledIndices []int
		// Indicies of jobs that should have their key in unfeasibleSchedulingKeys
		ExpectedUnfeasibleSchedulingKeyIndicies []int
		// The jctx we expect to be returned as preempted jctx
		ExpectedPreemptedJctx []*context.JobSchedulingContext
		// If an error is expected to be returned
		ExpectError bool
	}{
		"only schedule queues below their fairshare": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("100", "2000Gi"),
			InitialAllocatedByQueue: map[string]internaltypes.ResourceList{
				"B": testfixtures.CpuMem("60", "1000Gi"), // Over half, so queue is above its fairshare
			},
			Queues: armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
				testfixtures.N1Cpu4GiJobs("B", testfixtures.PriorityClass1, 10),
			),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 9),
		},
		"only schedule jobs that won't put the queue above fairshare": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Queue A fairshare is 5, only expect 5 scheduled jobs
			ExpectedScheduledIndices: testfixtures.IntRange(0, 4),
		},
		"should respect global rate limit": {
			SchedulingConfig:        testfixtures.WithGlobalSchedulingRateLimiterConfig(1, 1, schedulerConfig),
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Only schedules 1, to respect global limit
			ExpectedScheduledIndices: testfixtures.IntRange(0, 0),
		},
		"should respect queue rate limit": {
			SchedulingConfig:        testfixtures.WithPerQueueSchedulingLimiterConfig(2, 2, schedulerConfig),
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Only schedules 2, to respect queue limit
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
		},
		"should not scheduled on cordoned queues": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(withCordoned(testfixtures.SingleQueuePriorityOne("A")), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Queue A fairshare is 5, only expect 5 scheduled jobs
			ExpectedScheduledIndices: []int{},
		},
		"should respect MaximumJobsPerRound": {
			SchedulingConfig:        testfixtures.WithOptimiserConfig(testfixtures.TestPool, createOptimiserConfig(2, make(map[string]float64), nil), testfixtures.TestSchedulingConfig()),
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Only schedules 2 as MaximumJobsPerRound is 2
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
		},
		"should respect MaximumResourceFractionToSchedule": {
			SchedulingConfig:        testfixtures.WithOptimiserConfig(testfixtures.TestPool, createOptimiserConfig(10000, map[string]float64{"cpu": 0.3}, nil), testfixtures.TestSchedulingConfig()),
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			// Only schedules 3, as total resource is 10 and MaximumResourceFractionToSchedule for cpu is 30%
			ExpectedScheduledIndices: testfixtures.IntRange(0, 2),
		},
		"should respect MinimumJobSizeToSchedule": {
			SchedulingConfig: testfixtures.WithOptimiserConfig(
				testfixtures.TestPool, createOptimiserConfig(10000, map[string]float64{}, &armadaresource.ComputeResources{"cpu": resource.MustParse("4")}),
				testfixtures.TestSchedulingConfig()),
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("100", "500Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass1, 1),
			),
			// Only schedules large jobs, as smaller jobs don't meet the minimum size of 4 cpu
			ExpectedScheduledIndices: testfixtures.IntRange(10, 10),
		},
		"should not schedule jobs already successfully scheduled this round": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysSucceedGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			AlreadyScheduledThisRoundIndicies: testfixtures.IntRange(0, 1),
			// Shouldn't schedule 0 and 1, as they have already been successfully scheduled
			ExpectedScheduledIndices: testfixtures.IntRange(2, 6),
		},
		"should return preempted jobs": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysSucceedGangSchedulerWithPreemptedJobs,
			TotalSchedulingCapacity: testfixtures.CpuMem("10", "200Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2),
			),
			ExpectedScheduledIndices: testfixtures.IntRange(0, 1),
			// The stub always returns both, we scheduled 2 jobs so should get repeated values
			ExpectedPreemptedJctx: []*context.JobSchedulingContext{preemptedJctx1, preemptedJctx2, preemptedJctx1, preemptedJctx2},
		},
		"should utilise unfeasibleSchedulingKeys": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysFailsGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("100", "500Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
				testfixtures.N16Cpu128GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			ExpectedScheduledIndices:                []int{},
			ExpectedUnfeasibleSchedulingKeyIndicies: armadaslices.Concatenate(testfixtures.IntRange(0, 0), testfixtures.IntRange(10, 10)),
		},
		"should handle gang scheduler error": {
			SchedulingConfig:        schedulerConfig,
			GangScheduler:           alwaysErrorGangScheduler,
			TotalSchedulingCapacity: testfixtures.CpuMem("100", "500Gi"),
			Queues:                  armadaslices.Concatenate(testfixtures.SingleQueuePriorityOne("A"), testfixtures.SingleQueuePriorityOne("B")),
			Jobs: armadaslices.Concatenate(
				testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 10),
			),
			ExpectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobs := testfixtures.WithQueued(tc.Jobs)
			jobDb := testfixtures.NewJobDbWithJobs(jobs)
			constraints := schedulerconstraints.NewSchedulingConstraints(testfixtures.PoolNameLabel, tc.TotalSchedulingCapacity, tc.SchedulingConfig, tc.Queues)
			poolConfig := tc.SchedulingConfig.Pools[0]
			var minimumJobSizeToSchedule *internaltypes.ResourceList
			if poolConfig.Optimiser.MinimumJobSizeToSchedule != nil {
				minJobSize := testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(*poolConfig.Optimiser.MinimumJobSizeToSchedule)
				minimumJobSizeToSchedule = &minJobSize
			}
			queueScheduler := NewOptimisingQueueScheduler(
				jobDb.ReadTxn(),
				tc.GangScheduler,
				constraints,
				testfixtures.TestEmptyFloatingResources,
				tc.SchedulingConfig.MaxQueueLookback,
				tc.SchedulingConfig.EnablePreferLargeJobOrdering,
				false,
				minimumJobSizeToSchedule,
				poolConfig.Optimiser.MaximumJobsPerRound,
				poolConfig.Optimiser.MaximumResourceFractionToSchedule)

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				tc.TotalSchedulingCapacity,
				tc.SchedulingConfig,
			)
			require.NoError(t, err)

			sctx := context.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				rate.NewLimiter(
					rate.Limit(tc.SchedulingConfig.MaximumSchedulingRate),
					tc.SchedulingConfig.MaximumSchedulingBurst,
				),
				tc.TotalSchedulingCapacity,
			)
			for _, q := range tc.Queues {
				weight := 1.0 / float64(q.PriorityFactor)
				unlimitedDemand := testfixtures.CpuMem("10000", "100000Gi")
				err := sctx.AddQueueSchedulingContext(
					q.Name, weight, weight,
					map[string]internaltypes.ResourceList{testfixtures.PriorityClass2: tc.InitialAllocatedByQueue[q.Name]},
					unlimitedDemand,
					unlimitedDemand,
					rate.NewLimiter(
						rate.Limit(tc.SchedulingConfig.MaximumPerQueueSchedulingRate),
						tc.SchedulingConfig.MaximumPerQueueSchedulingBurst,
					),
				)
				require.NoError(t, err)
			}
			sctx.UpdateFairShares()

			for _, index := range tc.AlreadyScheduledThisRoundIndicies {
				job := tc.Jobs[index]
				jctx := context.JobSchedulingContextFromJob(job)
				// This is a hack for testing
				sctx.QueueSchedulingContexts[job.Queue()].SuccessfulJobSchedulingContexts[job.Id()] = jctx
			}

			ctx := armadacontext.Background()
			result, err := queueScheduler.Schedule(ctx, sctx)

			if tc.ExpectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Equal(t, gangSchedulerError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Len(t, result.ScheduledJobs, len(tc.ExpectedScheduledIndices))
				expectedScheduledJobs := []*jobdb.Job{}
				for _, index := range tc.ExpectedScheduledIndices {
					expectedScheduledJobs = append(expectedScheduledJobs, tc.Jobs[index])
				}
				expectedScheduledIds := armadaslices.Map(expectedScheduledJobs, func(job *jobdb.Job) string {
					return job.Id()
				})
				scheduledIds := armadaslices.Map(result.ScheduledJobs, func(jctx *context.JobSchedulingContext) string {
					return jctx.Job.Id()
				})
				slices.Sort(expectedScheduledIds)
				slices.Sort(scheduledIds)
				assert.Equal(t, expectedScheduledIds, scheduledIds)

				for _, index := range tc.ExpectedUnfeasibleSchedulingKeyIndicies {
					key := tc.Jobs[index].SchedulingKey()
					_, exists := sctx.UnfeasibleSchedulingKeys[key]
					assert.True(t, exists)
				}

				for i, preemptedJctx := range tc.ExpectedPreemptedJctx {
					assert.Equal(t, preemptedJctx.JobId, result.PreemptedJobs[i].JobId)
				}
			}
		})
	}
}

type StubGangScheduler struct {
	scheduleSuccess      bool
	scheduleError        error
	scheduleFailedReason string
	preemptedJobs        []*context.JobSchedulingContext
	t                    *testing.T
}

func (s *StubGangScheduler) Schedule(
	ctx *armadacontext.Context,
	gctx *context.GangSchedulingContext,
	sctx *context.SchedulingContext,
) (bool, []*context.JobSchedulingContext, string, error) {
	for _, jctx := range gctx.JobSchedulingContexts {
		jctx.PodSchedulingContext = &context.PodSchedulingContext{
			SchedulingMethod: context.ScheduledWithFairnessOptimiser,
			Created:          time.Now(),
			NodeId:           "node",
		}
	}
	_, err := sctx.AddGangSchedulingContext(gctx)
	require.NoError(s.t, err)
	return s.scheduleSuccess, s.preemptedJobs, s.scheduleFailedReason, s.scheduleError
}

func createOptimiserConfig(
	maximumJobsPerRound int,
	maximumFractionToSchedule map[string]float64,
	minimumJobSizeToSchedule *armadaresource.ComputeResources,
) *configuration.OptimiserConfig {
	return &configuration.OptimiserConfig{
		Enabled:                           true,
		Timeout:                           time.Second * 5,
		MaximumJobsPerRound:               maximumJobsPerRound,
		MaximumResourceFractionToSchedule: maximumFractionToSchedule,
		MinimumJobSizeToSchedule:          minimumJobSizeToSchedule,
	}
}

func withCordoned(queues []*api.Queue) []*api.Queue {
	for _, queue := range queues {
		queue.Cordoned = true
	}
	return queues
}
