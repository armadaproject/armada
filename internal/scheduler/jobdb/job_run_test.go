package jobdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var baseJobRun = CreateRun(
	uuid.New(),
	uuid.NewString(),
	5,
	"test-executor",
	"test-nodeId",
	"test-nodeName",
	false,
	false,
	false,
	false,
	false,
	false,
)

// Test methods that only have getters
func TestJobRun_TestGetter(t *testing.T) {
	assert.Equal(t, baseJobRun.id, baseJobRun.Id())
	assert.Equal(t, baseJobRun.created, baseJobRun.Created())
	assert.Equal(t, baseJobRun.executor, baseJobRun.Executor())
	assert.Equal(t, baseJobRun.nodeId, baseJobRun.NodeId())
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
	returnedRun := baseJobRun.WithReturned(true)
	assert.False(t, baseJobRun.Returned())
	assert.True(t, returnedRun.Returned())
}

func TestJobRun_TestRunAttempted(t *testing.T) {
	attemptedRun := baseJobRun.WithAttempted(true)
	assert.False(t, baseJobRun.RunAttempted())
	assert.True(t, attemptedRun.RunAttempted())
}

func TestDeepCopy(t *testing.T) {
	run := CreateRun(
		uuid.New(),
		"job id",
		1,
		"executor",
		"nodeId",
		"nodeName",
		true,
		true,
		true,
		true,
		true,
		true,
	)
	expected := CreateRun(
		run.id,
		"job id",
		1,
		"executor",
		"nodeId",
		"nodeName",
		true,
		true,
		true,
		true,
		true,
		true,
	)
	actual := run.DeepCopy()
	run.nodeId = "new nodeId"
	run.executor = "new executor"
	assert.Equal(t, expected, actual)
}
