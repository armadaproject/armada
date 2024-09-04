package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

var (
	nodeWithPool        = &schedulerobjects.Node{Pool: "node-pool"}
	nodeWithoutPool     = &schedulerobjects.Node{}
	executorWithPool    = &schedulerobjects.Executor{Pool: "executor-pool"}
	executorWithoutPool = &schedulerobjects.Executor{}

	runWithPool = testfixtures.JobDb.CreateRun(
		"",
		queuedJob.Id(),
		123,
		"test-executor",
		"test-executor-test-node",
		"test-node",
		"run-pool",
		pointer.Int32(5),
		false,
		false,
		false,
		false,
		false,
		false,
		false,
		false,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		false,
	)
	runWithoutPool = &jobdb.JobRun{}
)

func TestGetNodePool(t *testing.T) {
	assert.Equal(t, "node-pool", GetNodePool(nodeWithPool, executorWithPool))
	assert.Equal(t, "executor-pool", GetNodePool(nodeWithoutPool, executorWithPool))
	assert.Equal(t, DefaultPool, GetNodePool(nodeWithoutPool, executorWithoutPool))
}

func TestGetNodePool_NilInputs(t *testing.T) {
	assert.Equal(t, "node-pool", GetNodePool(nodeWithPool, nil))
	assert.Equal(t, "executor-pool", GetNodePool(nil, executorWithPool))
	assert.Equal(t, DefaultPool, GetNodePool(nil, nil))
}

func TestGetRunPool(t *testing.T) {
	assert.Equal(t, "run-pool", GetRunPool(runWithPool, nodeWithPool, executorWithPool))
	assert.Equal(t, "node-pool", GetRunPool(runWithoutPool, nodeWithPool, executorWithPool))
	assert.Equal(t, "executor-pool", GetRunPool(runWithoutPool, nodeWithoutPool, executorWithPool))
	assert.Equal(t, DefaultPool, GetRunPool(runWithoutPool, nodeWithoutPool, executorWithoutPool))
}

func TestGetRunPool_NilInputs(t *testing.T) {
	assert.Equal(t, "run-pool", GetRunPool(runWithPool, nil, nil))
	assert.Equal(t, "node-pool", GetRunPool(nil, nodeWithPool, executorWithPool))
	assert.Equal(t, "executor-pool", GetRunPool(nil, nil, executorWithPool))
	assert.Equal(t, DefaultPool, GetRunPool(nil, nil, nil))
}
