package runner

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSchedulingAlgo records the arguments it was called with and returns
// configured result/error. An optional onSchedule hook lets tests block or
// signal mid-call.
type fakeSchedulingAlgo struct {
	mu             sync.Mutex
	callCount      int
	lastTxn        *jobdb.Txn
	resultToReturn *scheduling.SchedulerResult
	errToReturn    error
	onSchedule     func()
}

func (f *fakeSchedulingAlgo) Schedule(_ *armadacontext.Context, _ map[string]internaltypes.ResourceList, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
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
	result   *scheduling.SchedulerResult
}

func (b *blockingSchedulingAlgo) Schedule(ctx *armadacontext.Context, _ map[string]internaltypes.ResourceList, _ *jobdb.Txn) (*scheduling.SchedulerResult, error) {
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
	algo := &fakeSchedulingAlgo{resultToReturn: &scheduling.SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)

	// Should not panic, should not invoke the algo.
	runner.Trigger()
	runner.Trigger()
	assert.Equal(t, 0, algo.calls())
}

func TestSyncSchedulingRunner_GetSchedulerResultDelegatesToAlgo(t *testing.T) {
	want := &scheduling.SchedulerResult{}
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
	algo := &fakeSchedulingAlgo{resultToReturn: &scheduling.SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)
	// Should not panic, should not invoke the algo.
	runner.Reset()
	assert.Equal(t, 0, algo.calls())
}
