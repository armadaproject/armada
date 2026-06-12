package runner

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

type fakeSchedulingAlgo struct {
	callCount atomic.Int64
	result    *scheduling.SchedulerResult
	err       error
}

func (f *fakeSchedulingAlgo) Schedule(_ *armadacontext.Context, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	f.callCount.Add(1)
	return f.result, f.err
}

func (f *fakeSchedulingAlgo) calls() int {
	return int(f.callCount.Load())
}

func TestSyncSchedulingRunner_GetSchedulerResultDelegatesToAlgo(t *testing.T) {
	expectedResult := &scheduling.SchedulerResult{}
	algo := &fakeSchedulingAlgo{result: expectedResult}
	runner := NewSyncSchedulingRunner(algo)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	result, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	require.NoError(t, err)
	assert.Same(t, expectedResult, result)
	assert.Equal(t, 1, algo.calls())
}

func TestSyncSchedulingRunner_GetSchedulerResultPropagatesError(t *testing.T) {
	expectedError := fmt.Errorf("expected error")
	algo := &fakeSchedulingAlgo{err: expectedError}
	runner := NewSyncSchedulingRunner(algo)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	txn := jobDb.WriteTxn()
	defer txn.Abort()

	_, err := runner.GetSchedulerResult(armadacontext.Background(), txn)
	assert.ErrorIs(t, err, expectedError)
}

func TestSyncSchedulingRunner_TriggerIsNoOp(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)

	runner.Trigger()
	assert.Equal(t, 0, algo.calls())
}

func TestSyncSchedulingRunner_IsAsyncFalse(t *testing.T) {
	runner := NewSyncSchedulingRunner(&fakeSchedulingAlgo{})
	assert.False(t, runner.IsAsync())
}

func TestSyncSchedulingRunner_ResetIsNoOp(t *testing.T) {
	algo := &fakeSchedulingAlgo{result: &scheduling.SchedulerResult{}}
	runner := NewSyncSchedulingRunner(algo)
	runner.Reset()
	assert.Equal(t, 0, algo.calls())
}
