package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

type failureCacheTestCase struct {
	name    string
	reason  string
	prepare func(*context.GangSchedulingContext)
	cached  bool
}

func failureCacheTestCases() []failureCacheTestCase {
	return []failureCacheTestCase{
		{name: "job does not fit", reason: constraints.JobDoesNotFitUnschedulableReason, cached: true},
		{name: "gang does not fit", reason: constraints.GangDoesNotFitUnschedulableReason, cached: true},
		{name: "gang exceeds global burst", reason: constraints.GangExceedsGlobalBurstSizeUnschedulableReason, cached: true},
		{name: "queue burst", reason: constraints.GangExceedsQueueBurstSizeUnschedulableReason},
		{name: "round limit", reason: constraints.MaximumResourcesScheduledUnschedulableReason},
		{name: "optimiser placement", reason: constraints.OptimiserNoPlacementUnschedulableReason},
		{name: "optimiser fairness", reason: constraints.OptimiserNoFairnessImprovementUnschedulableReason},
		{name: "unknown reason", reason: "unknown failure"},
		{name: "multiple jobs", reason: constraints.JobDoesNotFitUnschedulableReason, prepare: func(gctx *context.GangSchedulingContext) {
			gctx.JobSchedulingContexts = context.JobSchedulingContextsFromJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2))
		}},
		{name: "additional selector", reason: constraints.JobDoesNotFitUnschedulableReason, prepare: func(gctx *context.GangSchedulingContext) {
			gctx.JobSchedulingContexts[0].AdditionalNodeSelectors = map[string]string{"node": "restricted"}
		}},
		{name: "additional toleration", reason: constraints.JobDoesNotFitUnschedulableReason, prepare: func(gctx *context.GangSchedulingContext) {
			gctx.JobSchedulingContexts[0].AdditionalTolerations = []v1.Toleration{{Key: "extra"}}
		}},
		{name: "empty key", reason: constraints.JobDoesNotFitUnschedulableReason, prepare: func(gctx *context.GangSchedulingContext) {
			// A zero-value job supplies an available but empty key, unlike extra scheduling requirements.
			gctx.JobSchedulingContexts[0].Job = &jobdb.Job{}
		}},
	}
}

func TestGangScheduler_FailureCache(t *testing.T) {
	for _, tc := range failureCacheTestCases() {
		for _, skip := range []bool{false, true} {
			name := tc.name
			if skip {
				name += " with cache disabled"
			}
			t.Run(name, func(t *testing.T) {
				jctxs := context.JobSchedulingContextsFromJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1))
				gctx := context.NewGangSchedulingContext(jctxs)
				if tc.prepare != nil {
					tc.prepare(gctx)
				}
				sctx := newFailureCacheSchedulingContext(t, gctx.JobSchedulingContexts[0].Job.Queue())
				sch := &GangScheduler{schedulingContext: sctx, skipUnsuccessfulSchedulingKeyCheck: skip}
				require.NoError(t, sch.updateGangSchedulingContextOnFailure(gctx, false, tc.reason))
				if tc.cached && !skip {
					key, ok := jctxs[0].SchedulingKey()
					require.True(t, ok)
					require.Len(t, sctx.UnfeasibleSchedulingKeys, 1)
					assert.Same(t, jctxs[0], sctx.UnfeasibleSchedulingKeys[key])
				} else {
					assert.Empty(t, sctx.UnfeasibleSchedulingKeys)
				}
				for _, jctx := range gctx.JobSchedulingContexts {
					assert.Equal(t, tc.reason, jctx.UnschedulableReason)
					assert.Same(t, jctx, sctx.QueueSchedulingContexts[jctx.Job.Queue()].UnsuccessfulJobSchedulingContexts[jctx.JobId])
				}
			})
		}
	}
}

func TestGangScheduler_FailureCacheRetainsFirstContext(t *testing.T) {
	jctxs := context.JobSchedulingContextsFromJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2))
	sctx := newFailureCacheSchedulingContext(t, "A")
	sch := &GangScheduler{schedulingContext: sctx}
	require.NoError(t, sch.updateGangSchedulingContextOnFailure(context.NewGangSchedulingContext(jctxs[:1]), false, constraints.JobDoesNotFitUnschedulableReason))
	require.NoError(t, sch.updateGangSchedulingContextOnFailure(context.NewGangSchedulingContext(jctxs[1:]), false, constraints.GangDoesNotFitUnschedulableReason))
	require.Equal(t, jctxs[0].Job.SchedulingKey(), jctxs[1].Job.SchedulingKey())
	require.Len(t, sctx.UnfeasibleSchedulingKeys, 1)
	assert.Same(t, jctxs[0], sctx.UnfeasibleSchedulingKeys[jctxs[0].Job.SchedulingKey()])
	assert.Len(t, sctx.QueueSchedulingContexts["A"].UnsuccessfulJobSchedulingContexts, 2)
}

func TestGangScheduler_FailureCacheAfterBookkeeping(t *testing.T) {
	jctxs := context.JobSchedulingContextsFromJobs(testfixtures.N1Cpu4GiJobs("missing", testfixtures.PriorityClass1, 1))
	sctx := newFailureCacheSchedulingContext(t, "A")
	sch := &GangScheduler{schedulingContext: sctx}
	require.Error(t, sch.updateGangSchedulingContextOnFailure(context.NewGangSchedulingContext(jctxs), false, constraints.JobDoesNotFitUnschedulableReason))
	assert.Empty(t, sctx.UnfeasibleSchedulingKeys)
}

// These injected failures characterise the delegate interface. The production
// fairness optimiser currently returns only non-cacheable reasons.
func TestOptimisingQueueScheduler_SyntheticFailureCache(t *testing.T) {
	for _, tc := range failureCacheTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			sctx := newFailureCacheSchedulingContext(t, "A")
			jobs := testfixtures.WithQueued(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1))
			var attempted *context.JobSchedulingContext
			sch := newFailureCacheOptimisingScheduler(t, sctx, jobs, syntheticCacheFailureGangScheduler(func(gctx *context.GangSchedulingContext, _ *context.SchedulingContext) (string, error) {
				if tc.prepare != nil {
					tc.prepare(gctx)
				}
				attempted = gctx.JobSchedulingContexts[0]
				return tc.reason, nil
			}))
			result, err := sch.Schedule(armadacontext.Background(), sctx)
			require.NoError(t, err)
			assert.Empty(t, result.ScheduledJobs)
			require.NotNil(t, attempted)
			if tc.cached {
				require.Len(t, sctx.UnfeasibleSchedulingKeys, 1)
				assert.Same(t, attempted, sctx.UnfeasibleSchedulingKeys[attempted.Job.SchedulingKey()])
			} else {
				assert.Empty(t, sctx.UnfeasibleSchedulingKeys)
			}
		})
	}
}

func TestOptimisingQueueScheduler_SyntheticFailureCacheRetainsFirstContext(t *testing.T) {
	sctx := newFailureCacheSchedulingContext(t, "A")
	jobs := testfixtures.WithQueued(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 2))
	first := context.JobSchedulingContextFromJob(jobs[0])
	var attempted *context.JobSchedulingContext
	sch := newFailureCacheOptimisingScheduler(t, sctx, jobs[1:], syntheticCacheFailureGangScheduler(func(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (string, error) {
		// Seed an unfailed context after selecting the only candidate. This tests
		// retention of an existing entry, not production failure propagation.
		attempted = gctx.JobSchedulingContexts[0]
		require.Equal(t, first.Job.SchedulingKey(), gctx.JobSchedulingContexts[0].Job.SchedulingKey())
		sctx.UnfeasibleSchedulingKeys[first.Job.SchedulingKey()] = first
		return constraints.JobDoesNotFitUnschedulableReason, nil
	}))
	_, err := sch.Schedule(armadacontext.Background(), sctx)
	require.NoError(t, err)
	require.Len(t, sctx.UnfeasibleSchedulingKeys, 1)
	assert.Same(t, first, sctx.UnfeasibleSchedulingKeys[first.Job.SchedulingKey()])
	require.NotNil(t, attempted)
	assert.Empty(t, first.UnschedulableReason)
	assert.Empty(t, attempted.UnschedulableReason)
}

func TestOptimisingQueueScheduler_SyntheticFailureCacheIgnoresErrors(t *testing.T) {
	sctx := newFailureCacheSchedulingContext(t, "A")
	jobs := testfixtures.WithQueued(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 1))
	sch := newFailureCacheOptimisingScheduler(t, sctx, jobs, syntheticCacheFailureGangScheduler(func(_ *context.GangSchedulingContext, _ *context.SchedulingContext) (string, error) {
		return constraints.JobDoesNotFitUnschedulableReason, gangSchedulerError
	}))
	_, err := sch.Schedule(armadacontext.Background(), sctx)
	require.ErrorIs(t, err, gangSchedulerError)
	assert.Empty(t, sctx.UnfeasibleSchedulingKeys)
}

func TestQueuedGangIterator_ReusesFailureContextUntilCacheReset(t *testing.T) {
	jctxs := context.JobSchedulingContextsFromJobs(testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass1, 3))
	sctx := newFailureCacheSchedulingContext(t, "A")
	first := jctxs[0]
	first.UnschedulableReason = constraints.JobDoesNotFitUnschedulableReason
	first.PodSchedulingContext = &context.PodSchedulingContext{NodeId: "representative"}
	sctx.UnfeasibleSchedulingKeys[first.Job.SchedulingKey()] = first
	it := NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs[1:2]), 100, true)
	gctx, err := it.Peek()
	require.NoError(t, err)
	assert.Nil(t, gctx)
	assert.Equal(t, first.UnschedulableReason, jctxs[1].UnschedulableReason)
	assert.Same(t, first.PodSchedulingContext, jctxs[1].PodSchedulingContext)
	assert.Same(t, jctxs[1], sctx.QueueSchedulingContexts["A"].UnsuccessfulJobSchedulingContexts[jctxs[1].JobId])

	sctx.ClearUnfeasibleSchedulingKeys()
	assert.Empty(t, sctx.UnfeasibleSchedulingKeys)
	it = NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs[2:]), 100, true)
	gctx, err = it.Peek()
	require.NoError(t, err)
	require.NotNil(t, gctx)
	require.Len(t, gctx.JobSchedulingContexts, 1)
	assert.Same(t, jctxs[2], gctx.JobSchedulingContexts[0])
	assert.Empty(t, jctxs[2].UnschedulableReason)
	assert.Nil(t, jctxs[2].PodSchedulingContext)
}

func newFailureCacheSchedulingContext(t *testing.T, queue string) *context.SchedulingContext {
	t.Helper()
	sctx := context.NewSchedulingContext(testfixtures.TestPool, nil, rate.NewLimiter(rate.Inf, 1000), nil, testfixtures.CpuMem("100", "500Gi"))
	demand := testfixtures.CpuMem("10000", "100000Gi")
	require.NoError(t, sctx.AddQueueSchedulingContext(queue, 1, 1, nil, demand, demand, internaltypes.ResourceList{}, rate.NewLimiter(rate.Inf, 1000)))
	return sctx
}

func newFailureCacheOptimisingScheduler(t *testing.T, sctx *context.SchedulingContext, jobs []*jobdb.Job, gangScheduler syntheticCacheFailureGangScheduler) *OptimisingQueueScheduler {
	t.Helper()
	config := testfixtures.TestSchedulingConfig()
	cost, err := fairness.NewDominantResourceFairness(sctx.TotalResources, testfixtures.TestPool, config)
	require.NoError(t, err)
	sctx.FairnessCostProvider = cost
	sctx.UpdateFairShares()
	jobDb := testfixtures.NewJobDbWithJobs(jobs)
	schedulingConstraints := constraints.NewSchedulingConstraints(testfixtures.TestPool, sctx.TotalResources, config, []*api.Queue{{Name: "A"}})
	return NewOptimisingQueueScheduler(jobDb.ReadTxn(), gangScheduler, schedulingConstraints, testfixtures.TestEmptyFloatingResources, 100, false, nil, 100, nil)
}

type syntheticCacheFailureGangScheduler func(*context.GangSchedulingContext, *context.SchedulingContext) (string, error)

func (f syntheticCacheFailureGangScheduler) Schedule(_ *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error) {
	reason, err := f(gctx, sctx)
	return false, nil, reason, err
}
