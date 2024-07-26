package context

import (
	"testing"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/stretchr/testify/assert"
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
