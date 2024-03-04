package failureestimator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/optimisation/descent"
	"github.com/armadaproject/armada/internal/common/optimisation/nesterov"
)

func TestUpdate(t *testing.T) {
	fe, err := New(
		10,
		descent.MustNew(0.05),
		nesterov.MustNew(0.05, 0.2),
	)
	require.NoError(t, err)

	// Test initialisation.
	fe.Push("node", "queue", false)
	nodeParameterIndex, ok := fe.parameterIndexByNode["node"]
	require.True(t, ok)
	queueParameterIndex, ok := fe.parameterIndexByQueue["queue"]
	require.True(t, ok)
	require.Equal(t, 0, nodeParameterIndex)
	require.Equal(t, 1, queueParameterIndex)
	require.Equal(t, 0.5, fe.parameters.AtVec(0))
	require.Equal(t, 0.5, fe.parameters.AtVec(1))

	for i := 0; i < 100; i++ {
		fe.Push(fmt.Sprintf("node-%d", i), "queue-0", false)
	}
	nodeParameterIndex, ok = fe.parameterIndexByNode["node-99"]
	require.True(t, ok)
	queueParameterIndex, ok = fe.parameterIndexByQueue["queue-0"]
	require.True(t, ok)
	require.Equal(t, 2+100, nodeParameterIndex)
	require.Equal(t, 3, queueParameterIndex)
	require.Equal(t, 0.5, fe.parameters.AtVec(102))
	require.Equal(t, 0.5, fe.parameters.AtVec(3))

	// Test that the estimates move in the expected direction on failure.
	fe.Update()
	nodeSuccessProbability := fe.parameters.AtVec(0)
	queueSuccessProbability := fe.parameters.AtVec(1)
	assert.Greater(t, nodeSuccessProbability, eps)
	assert.Greater(t, queueSuccessProbability, eps)
	assert.Less(t, nodeSuccessProbability, 0.5-eps)
	assert.Less(t, queueSuccessProbability, 0.5-eps)

	// Test that the estimates move in the expected direction on success.
	fe.Push("node", "queue", true)
	fe.Update()
	assert.Greater(t, fe.parameters.AtVec(0), nodeSuccessProbability)
	assert.Greater(t, fe.parameters.AtVec(1), queueSuccessProbability)

	for i := 0; i < 1000; i++ {
		for i := 0; i < 10; i++ {
			fe.Push("node", "queue", false)
		}
		fe.Update()
	}
	assert.Greater(t, fe.parameters.AtVec(0), 0.0)
	assert.Greater(t, fe.parameters.AtVec(1), 0.0)
	assert.Less(t, fe.parameters.AtVec(0), 2*eps)
	assert.Less(t, fe.parameters.AtVec(1), 2*eps)

	for i := 0; i < 1000; i++ {
		for i := 0; i < 10; i++ {
			fe.Push("node", "queue", true)
		}
		fe.Update()
	}
	assert.Greater(t, fe.parameters.AtVec(0), 1-2*eps)
	assert.Greater(t, fe.parameters.AtVec(1), 1-2*eps)
	assert.Less(t, fe.parameters.AtVec(0), 1.0)
	assert.Less(t, fe.parameters.AtVec(1), 1.0)
}
