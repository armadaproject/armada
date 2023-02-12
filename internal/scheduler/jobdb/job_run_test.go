package jobdb

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

var baseJobRun = CreateRun(
	uuid.New(),
	5,
	"test-executor",
	false,
	false,
	false,
	false,
	false)

// Test methods that only have getters
func TestJobRun_TestGetter(t *testing.T) {
	assert.Equal(t, baseJobRun.id, baseJobRun.Id())
	assert.Equal(t, baseJobRun.created, baseJobRun.Created())
	assert.Equal(t, baseJobRun.executor, baseJobRun.Executor())
}

func TestJobRun_TestRunning(t *testing.T) {
	runningRun := baseJobRun.WithRunning(true)
	assert.False(t, baseJobRun.Running())
	assert.True(t, runningRun.Running())
}

func TestJobRun_TestSucceeded(t *testing.T) {
	succeededRun := baseJobRun.WithSucceeded(true)
	assert.False(t, baseJobRun.Succeeded())
	assert.True(t, succeededRun.Succeeded())
}

func TestJobRun_TestFailed(t *testing.T) {
	failedRun := baseJobRun.WithFailed(true)
	assert.False(t, baseJobRun.Failed())
	assert.True(t, failedRun.Failed())
}

func TestJobRun_TestCancelled(t *testing.T) {
	cancelledRun := baseJobRun.WithCancelled(true)
	assert.False(t, baseJobRun.Cancelled())
	assert.True(t, cancelledRun.Cancelled())
}

func TestJobRun_TestReturned(t *testing.T) {
	cancelledRun := baseJobRun.WithReturned(true)
	assert.False(t, baseJobRun.Returned())
	assert.True(t, cancelledRun.Returned())
}
