package context

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestJobSchedulingContext_SetAssignedNodeId(t *testing.T) {
	jctx := &JobSchedulingContext{}

	assert.Equal(t, "", jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	// Will not add a node selector if input is empty
	jctx.SetAssignedNodeId("")
	assert.Equal(t, "", jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	jctx.SetAssignedNodeId("node1")
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
}
