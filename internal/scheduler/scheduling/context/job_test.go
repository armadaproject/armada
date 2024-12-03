package context

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	ittestfixtures "github.com/armadaproject/armada/internal/scheduler/internaltypes/testfixtures"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestJobSchedulingContext_SetAssignedNode(t *testing.T) {
	jctx := &JobSchedulingContext{}

	assert.Nil(t, jctx.GetAssignedNode())
	assert.Empty(t, jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	// Will not add a node selector if input is nil
	jctx.SetAssignedNode(nil)
	assert.Nil(t, jctx.GetAssignedNode())
	assert.Empty(t, jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	n := ittestfixtures.TestSimpleNode("node1")
	jctx.SetAssignedNode(n)
	assert.Equal(t, n, jctx.GetAssignedNode())
	assert.Equal(t, "node1", jctx.GetAssignedNodeId())
	assert.Len(t, jctx.AdditionalNodeSelectors, 1)
	assert.Equal(t, map[string]string{configuration.NodeIdLabel: "node1"}, jctx.AdditionalNodeSelectors)
}

func TestJobSchedulingContext_IsHomePool(t *testing.T) {
	job := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	jctx := &JobSchedulingContext{Job: job}

	assert.True(t, jctx.IsHomeJob(testfixtures.TestPool))

	// Add run for the current pool
	job = job.WithNewRun("executor", "node-id", "node", testfixtures.TestPool, 1)
	jctx = &JobSchedulingContext{Job: job}
	assert.True(t, jctx.IsHomeJob(testfixtures.TestPool))

	// Add run for a different pool
	job = job.WithNewRun("executor", "node-id", "node", "other", 1)
	jctx = &JobSchedulingContext{Job: job}
	assert.False(t, jctx.IsHomeJob(testfixtures.TestPool))

	// Add returned run for a different pool
	job = job.WithNewRun("executor", "node-id", "node", "other", 1).WithQueued(true)
	jctx = &JobSchedulingContext{Job: job}
	assert.True(t, jctx.IsHomeJob(testfixtures.TestPool))
}
