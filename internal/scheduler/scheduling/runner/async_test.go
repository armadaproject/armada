package runner

import (
	"fmt"
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

const reconcileTestPool = "testPool"

func waitForResultReady(t *testing.T, runner *AsyncSchedulingRunner) {
	t.Helper()
	waitForState(t, runner, ResultReady)
}

func waitForState(t *testing.T, runner *AsyncSchedulingRunner, state RunState) {
	t.Helper()
	require.Eventually(t, func() bool {
		return runner.GetCurrentState() == state
	}, 5*time.Second, 5*time.Millisecond, "expected runner to transition to state %s", state)
}

func newTestRunner(t *testing.T, algo scheduling.SchedulingAlgo) (*AsyncSchedulingRunner, *jobdb.JobDb) {
	t.Helper()
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	t.Cleanup(cancel)
	return NewAsyncSchedulingRunner(ctx, algo, jobDb), jobDb
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ReturnsEmptyIfNoResultReady(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner, jobDb := newTestRunner(t, algo)
	// No Trigger — no result has been produced yet.
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, runner.GetCurrentState(), Idle)
	assert.Equal(t, 0, algo.calls())
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ReturnsResultFromAlgoIfTriggerWasCalled(t *testing.T) {
	expectedResult := &scheduling.SchedulerResult{}
	algo := &fakeSchedulingAlgo{result: expectedResult}
	runner, jobDb := newTestRunner(t, algo)

	// Trigger result to be produced
	runner.Trigger()

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	waitForResultReady(t, runner)

	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	assert.Equal(t, 1, algo.calls())
}

func TestAsyncSchedulingRunner_GetSchedulerResult_ClearsPendingResult(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner, jobDb := newTestRunner(t, algo)

	runner.Trigger()
	waitForResultReady(t, runner)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	// First call drains the result.
	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Second call (without another Trigger) sees no pending result.
	result, err = runner.GetSchedulerResult(armadacontext.Background(), txn)
	assert.NoError(t, err)
	assert.Nil(t, result)
	assert.Equal(t, runner.GetCurrentState(), Idle)
}

func TestAsyncSchedulingRunner_DropsTriggersWhileResultPending(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner, jobDb := newTestRunner(t, algo)

	// First Trigger produces a result.
	runner.Trigger()
	waitForResultReady(t, runner)

	// Subsequent Triggers should be dropped while the result is still pending —
	// the goroutine refuses to overwrite an unread result.
	for i := 0; i < 10; i++ {
		runner.Trigger()
	}
	assert.Equal(t, 1, algo.calls())

	// Drain the result and trigger again — now a fresh run should happen.
	txn := jobDb.WriteTxn()
	_, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)

	runner.Trigger()
	waitForResultReady(t, runner)
	assert.Equal(t, 2, algo.calls())
}

func TestAsyncSchedulingRunner_AlgoErrorPropagated(t *testing.T) {
	schedulingErr := fmt.Errorf("scheduling failed")
	algo := &fakeSchedulingAlgo{err: schedulingErr}
	runner, jobDb := newTestRunner(t, algo)

	runner.Trigger()
	waitForResultReady(t, runner)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	_, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	assert.ErrorIs(t, err, schedulingErr)

	// Error result is cleared — second call returns empty / no error.
	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestAsyncSchedulingRunner_ContextCancellation_StopsGoroutine(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// Run once to confirm the goroutine is alive.
	runner.Trigger()
	waitForResultReady(t, runner)

	// Drain so a subsequent Trigger isn't dropped.
	txn := jobDb.WriteTxn()
	defer txn.Abort()
	_, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Equal(t, 1, algo.calls())

	cancel()

	// Triggers after cancellation should be a noop
	// - They may block waiting on current cycle to finish
	//.- They should not trigger another cycle as the main loop should have stopped
	for i := 0; i < 10; i++ {
		runner.Trigger()
	}

	waitForState(t, runner, Stopped)
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
	runner, jobDb := newTestRunner(t, algo)

	// Produce a result and leave it pending.
	runner.Trigger()
	waitForResultReady(t, runner)

	runner.Reset()

	// Result should have been discarded — GetSchedulerResult returns nothing.
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestAsyncSchedulingRunner_Reset_CancelsInflightAndBlocks(t *testing.T) {
	algo := &blockingSchedulingAlgo{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	runner, jobDb := newTestRunner(t, algo)

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
	got, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestAsyncSchedulingRunner_FunctionalAfterReset(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner, jobDb := newTestRunner(t, algo)

	runner.Trigger()
	waitForResultReady(t, runner)
	assert.Equal(t, 1, algo.calls())
	runner.Reset()

	// Trigger again post-Reset and confirm the runner still works.
	runner.Trigger()
	waitForResultReady(t, runner)
	assert.Equal(t, 2, algo.calls())

	txn := jobDb.WriteTxn()
	defer txn.Abort()
	got, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.NotNil(t, got)
}

func TestAsyncSchedulingRunner_Reset_DropsPendingRequest(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	r := &AsyncSchedulingRunner{
		schedulingAlgo: algo,
		jobDb:          testfixtures.NewJobDb(testfixtures.TestResourceListFactory),
		wake:           make(chan struct{}, 1),
	}

	r.Trigger()
	require.Equal(t, RunRequested, r.GetCurrentState(), "Trigger should request a run")

	r.Reset()
	assert.Equal(t, Idle, r.GetCurrentState(), "Reset must drop the pending request")
	assert.Equal(t, 0, algo.calls(), "no run should have started")

	// Confirm runner is still usable: a fresh Trigger re-requests.
	r.Trigger()
	assert.Equal(t, RunRequested, r.GetCurrentState())
}

func TestReconcile_UpdateScheduledJobs(t *testing.T) {
	tests := map[string]struct {
		gang          bool
		currentStates []jobState
		expectKept    bool
	}{
		"non-gang still queued is kept":    {currentStates: []jobState{jobStateQueued}, expectKept: true},
		"non-gang now leased is dropped":   {currentStates: []jobState{jobStateLeased}, expectKept: false},
		"non-gang now terminal is dropped": {currentStates: []jobState{jobStateTerminal}, expectKept: false},
		"gang all still queued keeps whole gang": {
			gang: true, currentStates: []jobState{jobStateQueued, jobStateQueued}, expectKept: true,
		},
		"gang one member not actionable drops whole gang": {
			gang: true, currentStates: []jobState{jobStateQueued, jobStateTerminal}, expectKept: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheduledJobs := queuedJobs(len(tc.currentStates), tc.gang)
			poolResult := scheduledPoolResult(t, scheduledJobs)

			currentJobs := make([]*jobdb.Job, len(scheduledJobs))
			for i, job := range scheduledJobs {
				currentJobs[i] = inState(job, tc.currentStates[i])
			}
			txn := txnWithJobs(currentJobs...)

			result := getSchedulerResultFromAsyncRunner(t, poolResult, txn)
			assert.Len(t, result.PoolResults, 1)
			expectedJobs := 0
			if tc.expectKept {
				expectedJobs = len(scheduledJobs)
			}
			assert.Len(t, result.PoolResults[0].SchedulingResult.ScheduledJobs, expectedJobs)
			for _, job := range scheduledJobs {
				if tc.expectKept {
					assert.NotNil(t, result.PoolResults[0].SchedulingResult.SchedulingContext.QueueSchedulingContexts[testfixtures.TestQueue].SuccessfulJobSchedulingContexts[job.Id()])
				} else {
					assert.Nil(t, result.PoolResults[0].SchedulingResult.SchedulingContext.QueueSchedulingContexts[testfixtures.TestQueue].SuccessfulJobSchedulingContexts[job.Id()])
				}
			}
		})
	}
}

func TestReconcile_Reconciliation_PreemptedJobs(t *testing.T) {
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
					SchedulingContext: createSctx(t, nil, []*jobdb.Job{preemptedJob}),
					AdditionalSchedulingInfo: &scheduling.SchedulingInformation{
						EvictorResult: &scheduling.EvictorResult{
							EvictedJctxsByJobId: map[string]*schedulercontext.JobSchedulingContext{preemptedJob.Id(): jctx},
						},
					},
				},
			}
			txn := txnWithJobs(inState(preemptedJob, tc.currentState))

			result := getSchedulerResultFromAsyncRunner(t, poolResult, txn)

			assert.Len(t, result.PoolResults, 1)
			if tc.expectStillPreempted {
				assert.Len(t, result.PoolResults[0].SchedulingResult.PreemptedJobs, 1)
				assert.True(t, result.PoolResults[0].SchedulingResult.SchedulingContext.QueueSchedulingContexts[testfixtures.TestQueue].EvictedJobsById[preemptedJob.Id()])
			} else {
				assert.Empty(t, result.PoolResults[0].SchedulingResult.PreemptedJobs)
				assert.False(t, result.PoolResults[0].SchedulingResult.SchedulingContext.QueueSchedulingContexts[testfixtures.TestQueue].EvictedJobsById[preemptedJob.Id()])
				_, stillEvicted := result.PoolResults[0].SchedulingResult.AdditionalSchedulingInfo.EvictorResult.EvictedJctxsByJobId[preemptedJob.Id()]
				assert.False(t, stillEvicted)
			}
		})
	}
}

func TestGetSchedulerResult_Reconciliation_ReconciliationResult(t *testing.T) {
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

			result := getSchedulerResultFromAsyncRunner(t, poolResult, txn)

			assert.Len(t, result.PoolResults, 1)
			if tc.expectKept {
				assert.Len(t, result.PoolResults[0].ReconciliationResult.FailedJobs, 1)
				assert.Len(t, result.PoolResults[0].ReconciliationResult.PreemptedJobs, 1)
			} else {
				assert.Empty(t, result.PoolResults[0].ReconciliationResult.FailedJobs)
				assert.Empty(t, result.PoolResults[0].ReconciliationResult.PreemptedJobs)
			}
		})
	}
}

func TestReconcile_PoolWithNilSchedulingResult(t *testing.T) {
	failedJob := testfixtures.Test1Cpu4GiJob(testfixtures.TestQueue, testfixtures.TestDefaultPriorityClass)
	algoResult := &scheduling.PoolSchedulingResult{
		Name: reconcileTestPool,
		ReconciliationResult: &scheduling.ReconciliationResult{
			FailedJobs:    []*scheduling.FailedReconciliationResult{{Job: failedJob, Reason: "failed"}},
			PreemptedJobs: []*scheduling.FailedReconciliationResult{},
		},
	}
	expectedReconciledResult := &scheduling.PoolSchedulingResult{
		Name: reconcileTestPool,
		ReconciliationResult: &scheduling.ReconciliationResult{
			FailedJobs:    []*scheduling.FailedReconciliationResult{},
			PreemptedJobs: []*scheduling.FailedReconciliationResult{},
		},
	}

	// Job is terminal in the current txn, so the reconciliation decision is stale.
	txn := txnWithJobs(inState(failedJob, jobStateTerminal))

	result := getSchedulerResultFromAsyncRunner(t, algoResult, txn)
	assert.Len(t, result.PoolResults, 1)
	assert.Equal(t, expectedReconciledResult, result.PoolResults[0])
}

func getSchedulerResultFromAsyncRunner(t *testing.T, poolResult *scheduling.PoolSchedulingResult, updatedTxn *jobdb.Txn) *scheduling.SchedulerResult {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{PoolResults: []*scheduling.PoolSchedulingResult{poolResult}}}
	runner, _ := newTestRunner(t, algo)

	runner.Trigger()
	waitForResultReady(t, runner)

	result, err := runner.GetSchedulerResult(armadacontext.Background(), updatedTxn)
	require.NoError(t, err)
	assert.NotNil(t, result)

	return result
}

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

// createSctx builds a SchedulingContext for the test queue with the
// supplied jobs added as successfully scheduled.
func createSctx(t *testing.T, scheduledJobs []*jobdb.Job, preemptedJobs []*jobdb.Job) *schedulercontext.SchedulingContext {
	t.Helper()
	totalResources := testfixtures.TestResourceListFactory.MakeAllZero()
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, reconcileTestPool, testfixtures.TestSchedulingConfig())
	require.NoError(t, err)
	sctx := schedulercontext.NewSchedulingContext(reconcileTestPool, fairnessCostProvider, nil, totalResources)

	demand := internaltypes.ResourceList{}
	for _, job := range append(scheduledJobs, preemptedJobs...) {
		demand = demand.Add(job.AllResourceRequirements())
	}

	initialAllocation := map[string]internaltypes.ResourceList{}
	for _, job := range preemptedJobs {
		initialAllocation[job.PriorityClassName()] = initialAllocation[job.PriorityClassName()].Add(job.AllResourceRequirements())
	}

	err = sctx.AddQueueSchedulingContext(testfixtures.TestQueue, 1.0, 1.0, initialAllocation, demand, demand, internaltypes.ResourceList{}, nil)
	require.NoError(t, err)
	for _, job := range scheduledJobs {
		_, err := sctx.AddJobSchedulingContext(schedulercontext.JobSchedulingContextFromJob(job))
		require.NoError(t, err)
	}
	for _, job := range preemptedJobs {
		_, err := sctx.EvictJob(schedulercontext.JobSchedulingContextFromJob(job))
		require.NoError(t, err)
	}
	return sctx
}

// scheduledPoolResult builds a PoolSchedulingResult with the given jobs marked
// as scheduled, backed by a fresh SchedulingContext containing them.
func scheduledPoolResult(t *testing.T, scheduledJobs []*jobdb.Job) *scheduling.PoolSchedulingResult {
	t.Helper()
	return &scheduling.PoolSchedulingResult{
		SchedulingResult: &scheduling.SchedulingResult{
			ScheduledJobs:     jctxsFor(scheduledJobs...),
			SchedulingContext: createSctx(t, scheduledJobs, nil),
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

type blockingSchedulingAlgo struct {
	started  chan struct{} // closed once Schedule has been entered
	finished chan struct{} // closed after Schedule returns
}

func (b *blockingSchedulingAlgo) Schedule(ctx *armadacontext.Context, _ *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	close(b.started)
	<-ctx.Done()
	close(b.finished)
	return nil, ctx.Err()
}
