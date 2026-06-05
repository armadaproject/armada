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

// --- asyncSchedulingRunner -------------------------------------------------

// waitForCalls polls until the algo has been called at least n times, or
// fails the test. Used because the algo runs on a background goroutine.
func waitForCalls(t *testing.T, algo *fakeSchedulingAlgo, n int) {
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
	assert.NotNil(t, got)
	assert.Equal(t, &SchedulerResult{}, got)
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
	waitForCalls(t, algo, 1)

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
	waitForCalls(t, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	// First call drains the result.
	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)

	// Second call (without another Trigger) sees no pending result.
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Equal(t, &SchedulerResult{}, got)
}

func TestAsyncSchedulingRunner_BackPressureDropsTriggersWhileResultPending(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	// First Trigger produces a result.
	runner.Trigger()
	waitForCalls(t, algo, 1)

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
	waitForCalls(t, algo, 2)
}

func TestAsyncSchedulingRunner_AlgoErrorPropagatedAndCleared(t *testing.T) {
	wantErr := errors.New("scheduling failed")
	algo := &fakeSchedulingAlgo{errToReturn: wantErr}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, algo, 1)

	txn := jobDb.WriteTxn()
	defer txn.Abort()

	_, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	assert.ErrorIs(t, err, wantErr)

	// Error result is cleared — second call returns empty / no error.
	got, err := runner.GetSchedulerResult(armadacontext.Background(), nil, txn)
	require.NoError(t, err)
	assert.Equal(t, &SchedulerResult{}, got)
}

func TestAsyncSchedulingRunner_AlgoReceivesDryRunTxnNotCallerTxn(t *testing.T) {
	algo := &fakeSchedulingAlgo{resultToReturn: &SchedulerResult{}}
	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)

	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	runner := NewAsyncSchedulingRunner(ctx, algo, jobDb)

	runner.Trigger()
	waitForCalls(t, algo, 1)

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
	waitForCalls(t, algo, 1)

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
