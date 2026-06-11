package runner

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

// waitForCalls polls until the algo has been called at least n times, or
// fails the test. Used because the algo runs on a background goroutine.
func waitForCalls(t *testing.T, _ SchedulingRunner, algo *fakeSchedulingAlgo, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return algo.calls() >= n
	}, 5*time.Second, 5*time.Millisecond, "expected algo to be called at least %d times", n)
}

func waitForResult(t *testing.T, runner SchedulingRunner, txn *jobdb.Txn) (*scheduling.SchedulerResult, error, bool) {
	t.Helper()

	// Wait for max of 5 seconds, sleeping 50ms between checks
	for i := 0; i < 100; i++ {
		result, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
		if result != nil || err != nil {
			return result, err, false
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil, errors.New("timed out waiting for result"), true
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ReturnsEmptyIfNoResultReady(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// No Trigger — no result has been produced yet.
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	result, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, 0, algo.calls())
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ReturnsResultFromAlgoIfTriggerWasCalled(t *testing.T) {
	expectedResult := &scheduling.SchedulerResult{}
	algo := &fakeSchedulingAlgo{result: expectedResult}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// Trigger result to be produced
	runner.Trigger()

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	result, err, timedOut := waitForResult(t, runner, txn)
	require.False(t, timedOut)
	require.NoError(t, err)
	assert.Same(t, expectedResult, result)
	assert.Equal(t, 1, algo.calls())
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ClearsPendingResult(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	// First call drains the result.
	result, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Second call (without another Trigger) sees no pending result.
	result, err = runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestAsyncSchedulingRunner_BackPressureDropsTriggersWhileResultPending(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// First Trigger produces a result.
	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	// Subsequent Triggers should be dropped while the result is still pending —
	// the goroutine refuses to overwrite an unread result.
	for i := 0; i < 10; i++ {
		runner.Trigger()
	}
	assert.Equal(t, 1, algo.calls())

	// Drain the result and trigger again — now a fresh run should happen.
	txn := jobDb.WriteTxn()
	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)

	runner.Trigger()
	waitForCalls(t, runner, algo, 2)
}

func TestAsyncSchedulingRunner_AlgoErrorPropagatedAndCleared(t *testing.T) {
	wantErr := errors.New("scheduling failed")
	algo := &fakeSchedulingAlgo{err: wantErr}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()

	waitForCalls(t, runner, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	assert.ErrorIs(t, err, wantErr)

	// Error result is cleared — second call returns empty / no error.
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestAsyncSchedulingRunner_ContextCancellation_StopsGoroutine(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// Run once to confirm the goroutine is alive.
	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	// Drain so a subsequent Trigger isn't dropped by back-pressure.
	txn := jobDb.WriteTxn()
	defer txn.Abort()
	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)

	cancel()
	// Give the goroutine time to observe ctx.Done().
	time.Sleep(50 * time.Millisecond)

	// Triggers after cancellation should be a noop
	// - They may block waiting on current cycle to finish
	//.- They should not trigger another cycle as the main loop should have stopped
	for i := 0; i < 10; i++ {
		runner.Trigger()
	}
	// TODO wait on scheduling result, prove timeout
	assert.Equal(t, 1, algo.calls())
}

func TestAsyncSchedulingRunner_IsAsyncTrue(t *testing.T) {
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, nil, nil)
	assert.True(t, runner.IsAsync())
}

func TestAsyncSchedulingRunner_Reset_ClearsPendingResult(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// Produce a result and leave it pending.
	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	runner.Reset()

	// Result should have been discarded — GetSchedulerResult returns nothing.
	txn := jobDb.WriteTxn()

	result, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestAsyncSchedulingRunner_Reset_CancelsInflightAndBlocks(t *testing.T) {
	algo := &blockingSchedulingAlgo{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	// Wait for the goroutine to actually be inside Schedule.
	<-algo.started

	runner.Reset()

	select {
	case <-algo.finished:
	default:
		t.Fatal("Reset returned before in-flight Schedule had finished")
	}

	// Result from the cancelled run must NOT be available.
	txn := jobDb.WriteTxn()
	defer txn.Abort()
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestAsyncSchedulingRunner_FunctionalAfterReset(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, runner, algo, 1)
	runner.Reset()

	// Trigger again post-Reset and confirm the runner still works.
	runner.Trigger()
	waitForCalls(t, runner, algo, 2)

	txn := jobDb.WriteTxn()
	defer txn.Abort()
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.NotNil(t, got)
}

// --- reconcile -------------------------------------------------------------
//
// The reconcile helpers re-validate an async scheduling result against the
// current jobDb state, dropping decisions that have gone stale since the
// background run produced them. The helpers are independent of the runner's
// lifecycle state, so they're exercised against a bare runner.

const reconcileTestPool = "testPool"

// jobState describes the state a job should be in within the "current" txn
// that reconcile validates against.
type jobState int

const (
	// queued: still queued, not terminal — a scheduled decision is still valid.
	jobStateQueued jobState = iota
	// leased: no longer queued (already running) — a scheduled decision is stale.
	jobStateLeased
	// terminal: succeeded — any decision is stale.
	jobStateTerminal
)

// inState returns a copy of job transitioned into the requested state. It
// represents how the job looks in the "current" txn that reconcile validates
// against. The id is preserved so the txn lookup inside reconcile resolves to it.
func inState(job *jobdb.Job, state jobState) *jobdb.Job {
	switch state {
	case jobStateQueued:
		return job.WithQueued(true)
	case jobStateLeased:
		return job.WithQueued(false).WithNewRun("executor", "node", "node", reconcileTestPool, 0)
	case jobStateTerminal:
		return job.WithQueued(false).WithSucceeded(true)
	default:
		panic("unknown job state")
	}
}

// reconcileSctx builds a SchedulingContext for the test queue with the
// supplied jobs added as successfully scheduled.
func reconcileSctx(t *testing.T, scheduledJobs ...*jobdb.Job) *schedulercontext.SchedulingContext {
	t.Helper()
	totalResources := testfixtures.TestResourceListFactory.MakeAllZero()
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, reconcileTestPool, testfixtures.TestSchedulingConfig())
	require.NoError(t, err)
	sctx := schedulercontext.NewSchedulingContext(reconcileTestPool, fairnessCostProvider, nil, totalResources)
	err = sctx.AddQueueSchedulingContext(testfixtures.TestQueue, 1.0, 1.0, nil, internaltypes.ResourceList{}, internaltypes.ResourceList{}, internaltypes.ResourceList{}, nil)
	require.NoError(t, err)
	for _, job := range scheduledJobs {
		_, err := sctx.AddJobSchedulingContext(schedulercontext.JobSchedulingContextFromJob(job))
		require.NoError(t, err)
	}
	return sctx
}

// scheduledPoolResult builds a PoolSchedulingResult with the given jobs marked
// as scheduled, backed by a fresh SchedulingContext containing them.
func scheduledPoolResult(t *testing.T, scheduledJobs ...*jobdb.Job) *scheduling.PoolSchedulingResult {
	t.Helper()
	return &scheduling.PoolSchedulingResult{
		SchedulingResult: &scheduling.SchedulingResult{
			ScheduledJobs:     jctxsFor(scheduledJobs...),
			SchedulingContext: reconcileSctx(t, scheduledJobs...),
		},
	}
}

// txnWithJobs returns a jobDb txn containing the given jobs.
func txnWithJobs(jobs ...*jobdb.Job) *jobdb.Txn {
	return testfixtures.NewJobDbWithJobs(jobs).WriteTxn()
}

func jctxsFor(jobs ...*jobdb.Job) []*schedulercontext.JobSchedulingContext {
	return schedulercontext.JobSchedulingContextsFromJobs(jobs)
}

// queuedJobs returns n queued jobs in the test queue, optionally bound together
// as a single gang.
func queuedJobs(n int, gang bool) []*jobdb.Job {
	jobs := make([]*jobdb.Job, n)
	for i := range jobs {
		jobs[i] = testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass).WithQueued(true)
	}
	if gang {
		jobs = testfixtures.WithGangJobDetails(jobs, "gang-1", n, "")
	}
	return jobs
}

func TestReconcile_UpdateScheduledJobs(t *testing.T) {
	tests := map[string]struct {
		gang bool
		// currentStates is the state of each scheduled job in the "current"
		// txn, in order. Its length is the number of jobs scheduled.
		currentStates []jobState
		expectKept    int
	}{
		"non-gang still queued is kept":    {currentStates: []jobState{jobStateQueued}, expectKept: 1},
		"non-gang now leased is dropped":   {currentStates: []jobState{jobStateLeased}, expectKept: 0},
		"non-gang now terminal is dropped": {currentStates: []jobState{jobStateTerminal}, expectKept: 0},
		"gang all still queued keeps whole gang": {
			gang: true, currentStates: []jobState{jobStateQueued, jobStateQueued}, expectKept: 2,
		},
		"gang one member not actionable drops whole gang": {
			gang: true, currentStates: []jobState{jobStateQueued, jobStateTerminal}, expectKept: 0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheduledJobs := queuedJobs(len(tc.currentStates), tc.gang)
			poolResult := scheduledPoolResult(t, scheduledJobs...)

			currentJobs := make([]*jobdb.Job, len(scheduledJobs))
			for i, job := range scheduledJobs {
				currentJobs[i] = inState(job, tc.currentStates[i])
			}
			txn := txnWithJobs(currentJobs...)

			r := &asyncSchedulingRunner{}
			require.NoError(t, r.updateScheduledJobs(armadacontext.Background(), txn, poolResult))

			assert.Len(t, poolResult.SchedulingResult.ScheduledJobs, tc.expectKept)
		})
	}
}

func TestReconcile_UpdatePreemptedJobs(t *testing.T) {
	tests := map[string]struct {
		currentState         jobState
		expectStillPreempted bool
	}{
		"still leased is kept":    {currentState: jobStateLeased, expectStillPreempted: true},
		"now terminal is dropped": {currentState: jobStateTerminal, expectStillPreempted: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// A preempted job is one that was running; model it as leased in the result's sctx.
			preemptedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass).
				WithQueued(false).WithNewRun("executor", "node", "node", reconcileTestPool, 0)
			jctx := schedulercontext.JobSchedulingContextFromJob(preemptedJob)
			poolResult := &scheduling.PoolSchedulingResult{
				SchedulingResult: &scheduling.SchedulingResult{
					PreemptedJobs:     []*schedulercontext.JobSchedulingContext{jctx},
					SchedulingContext: reconcileSctx(t, preemptedJob),
					AdditionalSchedulingInfo: &scheduling.SchedulingInformation{
						EvictorResult: &scheduling.EvictorResult{
							EvictedJctxsByJobId: map[string]*schedulercontext.JobSchedulingContext{preemptedJob.Id(): jctx},
						},
					},
				},
			}
			txn := txnWithJobs(inState(preemptedJob, tc.currentState))

			r := &asyncSchedulingRunner{}
			err := r.updatePreemptedJobs(armadacontext.Background(), txn, poolResult)
			require.NoError(t, err)

			if tc.expectStillPreempted {
				assert.Len(t, poolResult.SchedulingResult.PreemptedJobs, 1)
			} else {
				assert.Empty(t, poolResult.SchedulingResult.PreemptedJobs)
				_, stillEvicted := poolResult.SchedulingResult.AdditionalSchedulingInfo.EvictorResult.EvictedJctxsByJobId[preemptedJob.Id()]
				assert.False(t, stillEvicted, "stale preemption must be removed from the evictor result")
			}
		})
	}
}

func TestReconcile_UpdateReconciliationResult(t *testing.T) {
	tests := map[string]struct {
		currentState jobState
		expectKept   bool
	}{
		"actionable job is kept":  {currentState: jobStateLeased, expectKept: true},
		"terminal job is dropped": {currentState: jobStateTerminal, expectKept: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			failedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass)
			preemptedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass)
			poolResult := &scheduling.PoolSchedulingResult{
				ReconciliationResult: &scheduling.ReconciliationResult{
					FailedJobs:    []*scheduling.FailedReconciliationResult{{Job: failedJob, Reason: "failed"}},
					PreemptedJobs: []*scheduling.FailedReconciliationResult{{Job: preemptedJob, Reason: "preempted"}},
				},
			}
			txn := txnWithJobs(
				inState(failedJob, tc.currentState),
				inState(preemptedJob, tc.currentState),
			)

			r := &asyncSchedulingRunner{}
			r.updateReconciliationResult(armadacontext.Background(), txn, poolResult)

			if tc.expectKept {
				assert.Len(t, poolResult.ReconciliationResult.FailedJobs, 1)
				assert.Len(t, poolResult.ReconciliationResult.PreemptedJobs, 1)
			} else {
				assert.Empty(t, poolResult.ReconciliationResult.FailedJobs)
				assert.Empty(t, poolResult.ReconciliationResult.PreemptedJobs)
			}
		})
	}
}

// TestReconcile_PoolWithNilSchedulingResult covers errored or scheduling-disabled
// pools: runPoolSchedulingRound leaves SchedulingResult nil for those, but their
// ReconciliationResult is still populated and must be reconciled. reconcile must
// skip the scheduling-decision steps (which dereference SchedulingResult) without
// panicking, while still filtering the reconciliation decisions.
func TestReconcile_PoolWithNilSchedulingResult(t *testing.T) {
	failedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass)
	result := &scheduling.SchedulerResult{
		PoolResults: []*scheduling.PoolSchedulingResult{
			{
				Name:             reconcileTestPool,
				SchedulingResult: nil, // errored / disabled pool
				ReconciliationResult: &scheduling.ReconciliationResult{
					FailedJobs:    []*scheduling.FailedReconciliationResult{{Job: failedJob, Reason: "failed"}},
					PreemptedJobs: []*scheduling.FailedReconciliationResult{},
				},
			},
		},
	}
	// Job is terminal in the current txn, so the reconciliation decision is stale.
	txn := txnWithJobs(inState(failedJob, jobStateTerminal))

	r := &asyncSchedulingRunner{}
	got, err := r.reconcile(armadacontext.Background(), txn, result)
	require.NoError(t, err)
	require.Same(t, result, got)
	assert.Empty(t, result.PoolResults[0].ReconciliationResult.FailedJobs,
		"stale reconciliation decision should be dropped even on a pool with no SchedulingResult")
}

type blockingSchedulingAlgo struct {
	started  chan struct{} // closed once Schedule has been entered
	finished chan struct{} // closed after Schedule returns
}

func (b *blockingSchedulingAlgo) Schedule(ctx *armadacontext.Context, _ map[string]internaltypes.ResourceList, _ *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	close(b.started)
	<-ctx.Done()
	close(b.finished)
	return nil, ctx.Err()
}
