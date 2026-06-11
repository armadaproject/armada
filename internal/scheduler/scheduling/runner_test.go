package scheduling

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

// fakeSchedulingAlgo records the arguments it was called with and returns
// configured result/error. An optional onSchedule hook lets tests block or
// signal mid-call.
type fakeSchedulingAlgo struct {
	mu             sync.Mutex
	callCount      int
	lastTxn        *jobdb.Txn
	resultToReturn *SchedulerResult
	errToReturn    error
	onSchedule     func()
}

func (f *fakeSchedulingAlgo) Schedule(_ *armadacontext.Context, _ map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*SchedulerResult, error) {
	f.mu.Lock()
	f.callCount++
	f.lastTxn = txn
	cb := f.onSchedule
	res := f.resultToReturn
	err := f.errToReturn
	f.mu.Unlock()
	if cb != nil {
		cb()
	}
	return res, err
}

func (f *fakeSchedulingAlgo) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

// blockingSchedulingAlgo blocks inside Schedule until ctx is cancelled, then
// sleeps briefly before returning so the test can detect whether the caller
// of Reset waited for the in-flight run to actually return.
type blockingSchedulingAlgo struct {
	started  chan struct{} // closed once Schedule has been entered
	finished chan struct{} // closed after Schedule returns
	result   *SchedulerResult
}

func (b *blockingSchedulingAlgo) Schedule(ctx *armadacontext.Context, _ map[string]internaltypes.ResourceList, _ *jobdb.Txn) (*SchedulerResult, error) {
	close(b.started)
	<-ctx.Done()
	// Simulate non-trivial cleanup work after cancellation. Reset must
	// wait for Schedule to return, not just observe ctx cancellation.
	time.Sleep(50 * time.Millisecond)
	close(b.finished)
	return nil, ctx.Err()
}

// --- syncSchedulingRunner --------------------------------------------------

func TestSyncSchedulingRunner_TriggerIsNoOp(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)

	// Should not panic, should not invoke the algo.
	runner.Trigger()
	runner.Trigger()
	assert.Equal(t, 0, algo.calls())
}

func TestSyncSchedulingRunner_GetSchedulerResultDelegatesToAlgo(t *testing.T) {
	want := &SchedulerResult{}
	algo := &fakeSchedulingAlgo{resultToReturn: want}
	runner := NewSyncSchedulingRunner(algo)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Same(t, want, got)
	assert.Equal(t, 1, algo.calls())
	assert.Same(t, txn, algo.lastTxn, "sync runner should pass caller's txn through to the algo")
}

func TestSyncSchedulingRunner_GetSchedulerResultPropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	algo := &fakeSchedulingAlgo{errToReturn: wantErr}
	runner := NewSyncSchedulingRunner(algo)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	assert.ErrorIs(t, err, wantErr)
}

func TestSyncSchedulingRunner_IsAsyncFalse(t *testing.T) {
	runner := NewSyncSchedulingRunner(&fakeSchedulingAlgo{})
	assert.False(t, runner.IsAsync())
}

func TestSyncSchedulingRunner_ResetIsNoOp(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)
	// Should not panic, should not invoke the algo.
	runner.Reset()
	assert.Equal(t, 0, algo.calls())
}

// --- asyncSchedulingRunner -------------------------------------------------

// waitForCalls polls until the algo has been called at least n times, or
// fails the test. Used because the algo runs on a background goroutine.
func waitForCalls(t *testing.T, _ SchedulingRunner, algo *fakeSchedulingAlgo, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return algo.calls() >= n
	}, 2*time.Second, 5*time.Millisecond, "expected algo to be called at least %d times", n)
}

func TestAsyncSchedulingRunner_GetSchedulerResultBeforeAnyRunReturnsEmpty(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// No Trigger — no result has been produced yet.
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, got, "no run has completed, so no result should be returned")
	assert.Equal(t, 0, algo.calls(), "algo should not be invoked from GetSchedulerResult in async mode")
}

func TestAsyncSchedulingRunner_TriggerProducesResultThatGetSchedulerResultReturns(t *testing.T) {
	want := &SchedulerResult{}
	algo := &fakeSchedulingAlgo{resultToReturn: want}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Same(t, want, got)
}

func TestAsyncSchedulingRunner_GetSchedulerResultClearsPendingResult(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	// First call drains the result.
	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)

	// Second call (without another Trigger) sees no pending result.
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestAsyncSchedulingRunner_BackPressureDropsTriggersWhileResultPending(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
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
	// Give any erroneous extra calls a chance to land.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, algo.calls(), "extra triggers must be dropped while a result is pending")

	// Drain the result and trigger again — now a fresh run should happen.
	txn := jobDb.WriteTxn()
	defer txn.Abort()
	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)

	runner.Trigger()
	waitForCalls(t, runner, algo, 2)
}

func TestAsyncSchedulingRunner_AlgoErrorPropagatedAndCleared(t *testing.T) {
	wantErr := errors.New("scheduling failed")
	algo := &fakeSchedulingAlgo{errToReturn: wantErr}
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

func TestAsyncSchedulingRunner_AlgoReceivesDryRunTxnNotCallerTxn(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, runner, algo, 1)

	callerTxn := jobDb.WriteTxn()
	defer callerTxn.Abort()

	algo.mu.Lock()
	algoTxn := algo.lastTxn
	algo.mu.Unlock()
	assert.NotSame(t, callerTxn, algoTxn, "async runner must schedule against an isolated dry-run txn, not the caller's")
}

func TestAsyncSchedulingRunner_ContextCancellationStopsGoroutine(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
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

	// Triggers after cancellation either drop (channel send blocks because
	// nothing's reading) or are absorbed by the dead goroutine — either way,
	// the algo call count should not increase.
	before := algo.calls()
	for i := 0; i < 10; i++ {
		runner.Trigger()
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, before, algo.calls(), "no algo calls should occur after ctx is cancelled")
}

func TestAsyncSchedulingRunner_IsAsyncTrue(t *testing.T) {
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, &fakeSchedulingAlgo{}, jobDb)
	assert.True(t, runner.IsAsync())
}

func TestAsyncSchedulingRunner_ResetClearsPendingResult(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
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
	defer txn.Abort()
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestAsyncSchedulingRunner_ResetCancelsInflightAndBlocks(t *testing.T) {
	algo := &blockingSchedulingAlgo{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
		result:   &SchedulerResult{},
	}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	// Wait for the goroutine to actually be inside Schedule.
	<-algo.started

	// Reset should cancel the in-flight Schedule and block until it has
	// returned (the algo sleeps 50ms after observing cancellation, so a
	// non-blocking Reset would race ahead of `finished`).
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
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
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
func scheduledPoolResult(t *testing.T, scheduledJobs ...*jobdb.Job) *PoolSchedulingResult {
	t.Helper()
	return &PoolSchedulingResult{
		SchedulingResult: &SchedulingResult{
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
			poolResult := &PoolSchedulingResult{
				SchedulingResult: &SchedulingResult{
					PreemptedJobs:     []*schedulercontext.JobSchedulingContext{jctx},
					SchedulingContext: reconcileSctx(t, preemptedJob),
					AdditionalSchedulingInfo: &SchedulingInformation{
						EvictorResult: &EvictorResult{
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
			poolResult := &PoolSchedulingResult{
				ReconciliationResult: &ReconciliationResult{
					FailedJobs:    []*FailedReconciliationResult{{Job: failedJob, Reason: "failed"}},
					PreemptedJobs: []*FailedReconciliationResult{{Job: preemptedJob, Reason: "preempted"}},
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
	result := &SchedulerResult{
		PoolResults: []*PoolSchedulingResult{
			{
				Name:             reconcileTestPool,
				SchedulingResult: nil, // errored / disabled pool
				ReconciliationResult: &ReconciliationResult{
					FailedJobs:    []*FailedReconciliationResult{{Job: failedJob, Reason: "failed"}},
					PreemptedJobs: []*FailedReconciliationResult{},
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
