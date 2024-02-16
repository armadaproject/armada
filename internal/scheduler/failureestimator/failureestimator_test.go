package failureestimator

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	successProbabilityCordonThreshold := 0.05
	cordonTimeout := 10 * time.Minute
	equilibriumFailureRate := 0.1

	// Node decay rate and step size tests.
	fe, err := New(
		successProbabilityCordonThreshold,
		0,
		cordonTimeout,
		0,
		equilibriumFailureRate,
		eps,
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, fe.nodeFailureProbabilityDecayRate, 0.9999145148300861+eps)
	assert.GreaterOrEqual(t, fe.nodeFailureProbabilityDecayRate, 0.9999145148300861-eps)
	assert.LessOrEqual(t, fe.nodeStepSize, 0.0008142462434300169+eps)
	assert.GreaterOrEqual(t, fe.nodeStepSize, 0.0008142462434300169-eps)

	// Queue decay rate and step size tests.
	fe, err = New(
		0,
		successProbabilityCordonThreshold,
		0,
		cordonTimeout,
		eps,
		equilibriumFailureRate,
	)
	require.NoError(t, err)
	assert.LessOrEqual(t, fe.queueFailureProbabilityDecayRate, 0.9999145148300861+eps)
	assert.GreaterOrEqual(t, fe.queueFailureProbabilityDecayRate, 0.9999145148300861-eps)
	assert.LessOrEqual(t, fe.queueStepSize, 0.0008142462434300169+eps)
	assert.GreaterOrEqual(t, fe.queueStepSize, 0.0008142462434300169-eps)
}

func TestDecay(t *testing.T) {
	successProbabilityCordonThreshold := 0.05
	cordonTimeout := 10 * time.Minute
	equilibriumFailureRate := 0.1

	// Node decay tests.
	fe, err := New(
		successProbabilityCordonThreshold,
		0,
		cordonTimeout,
		0,
		equilibriumFailureRate,
		eps,
	)
	require.NoError(t, err)

	fe.successProbabilityByNode["foo"] = 0.0
	fe.successProbabilityByNode["bar"] = 0.4
	fe.successProbabilityByNode["baz"] = 1.0

	fe.decay(0)
	assert.Equal(
		t,
		fe.successProbabilityByNode,
		map[string]float64{
			"foo": eps,
			"bar": 0.4,
			"baz": 1.0 - eps,
		},
	)

	fe.decay(1)
	assert.Equal(
		t,
		fe.successProbabilityByNode,
		map[string]float64{
			"foo": 1 - (1-eps)*math.Pow(fe.nodeFailureProbabilityDecayRate, 1),
			"bar": 1 - (1-0.4)*math.Pow(fe.nodeFailureProbabilityDecayRate, 1),
			"baz": 1.0 - eps,
		},
	)

	fe.decay(1e6)
	assert.Equal(
		t,
		fe.successProbabilityByNode,
		map[string]float64{
			"foo": 1.0 - eps,
			"bar": 1.0 - eps,
			"baz": 1.0 - eps,
		},
	)

	// Queue decay tests.
	fe, err = New(
		0,
		successProbabilityCordonThreshold,
		0,
		cordonTimeout,
		eps,
		equilibriumFailureRate,
	)
	require.NoError(t, err)

	fe.successProbabilityByQueue["foo"] = 0.0
	fe.successProbabilityByQueue["bar"] = 0.4
	fe.successProbabilityByQueue["baz"] = 1.0

	fe.decay(0)
	assert.Equal(
		t,
		fe.successProbabilityByQueue,
		map[string]float64{
			"foo": eps,
			"bar": 0.4,
			"baz": 1.0 - eps,
		},
	)

	fe.decay(1)
	assert.Equal(
		t,
		fe.successProbabilityByQueue,
		map[string]float64{
			"foo": 1 - (1-eps)*math.Pow(fe.queueFailureProbabilityDecayRate, 1),
			"bar": 1 - (1-0.4)*math.Pow(fe.queueFailureProbabilityDecayRate, 1),
			"baz": 1.0 - eps,
		},
	)

	fe.decay(1e6)
	assert.Equal(
		t,
		fe.successProbabilityByQueue,
		map[string]float64{
			"foo": 1.0 - eps,
			"bar": 1.0 - eps,
			"baz": 1.0 - eps,
		},
	)
}

func TestUpdate(t *testing.T) {
	successProbabilityCordonThreshold := 0.05
	cordonTimeout := 10 * time.Minute
	equilibriumFailureRate := 0.1

	fe, err := New(
		successProbabilityCordonThreshold,
		successProbabilityCordonThreshold,
		cordonTimeout,
		cordonTimeout,
		equilibriumFailureRate,
		equilibriumFailureRate,
	)
	require.NoError(t, err)

	fe.Update("node", "queue", false)
	nodeSuccessProbability, ok := fe.successProbabilityByNode["node"]
	require.True(t, ok)
	queueSuccessProbability, ok := fe.successProbabilityByQueue["queue"]
	require.True(t, ok)
	assert.Greater(t, nodeSuccessProbability, eps)
	assert.Greater(t, queueSuccessProbability, eps)
	assert.Less(t, nodeSuccessProbability, healthySuccessProbability-eps)
	assert.Less(t, queueSuccessProbability, healthySuccessProbability-eps)

	fe.Update("node", "queue", true)
	assert.Greater(t, fe.successProbabilityByNode["node"], nodeSuccessProbability)
	assert.Greater(t, fe.successProbabilityByQueue["queue"], queueSuccessProbability)

	for i := 0; i < 100000; i++ {
		fe.Update("node", "queue", false)
	}
	assert.Equal(t, fe.successProbabilityByNode["node"], eps)
	assert.Equal(t, fe.successProbabilityByQueue["queue"], eps)

	for i := 0; i < 100000; i++ {
		fe.Update("node", "queue", true)
	}
	assert.Equal(t, fe.successProbabilityByNode["node"], 1-eps)
	assert.Equal(t, fe.successProbabilityByQueue["queue"], 1-eps)
}
