package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestNodeType_GetId(t *testing.T) {
	nodeType := makeSut()

	assert.True(t, nodeType.GetId() != 0)
}

func TestNodeType_GetTaints(t *testing.T) {
	nodeType := makeSut()

	assert.Equal(t,
		[]v1.Taint{
			{Key: "taint1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
			{Key: "taint2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
		},
		nodeType.GetTaints(),
	)
}

func TestNodeType_FindMatchingUntoleratedTaint(t *testing.T) {
	nodeType := makeSut()
	taint, ok := nodeType.FindMatchingUntoleratedTaint([]v1.Toleration{{Key: "taint1", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}})

	assert.True(t, ok)
	assert.Equal(t,
		v1.Taint{Key: "taint2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
		taint)
}

func TestNodeTypeLabels(t *testing.T) {
	nodeType := makeSut()

	assert.Equal(t,
		map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
		nodeType.GetLabels(),
	)

	val1, ok1 := nodeType.GetLabelValue("label1")
	assert.Equal(t, val1, "value1")
	assert.True(t, ok1)

	val2, ok2 := nodeType.GetLabelValue("not-there")
	assert.Equal(t, val2, "")
	assert.False(t, ok2)

	assert.Equal(t,
		map[string]string{
			"label3": "",
		},
		nodeType.GetUnsetIndexedLabels(),
	)

	val3, ok3 := nodeType.GetUnsetIndexedLabelValue("label3")
	assert.Equal(t, val3, "")
	assert.True(t, ok3)

	val4, ok4 := nodeType.GetUnsetIndexedLabelValue("not-there")
	assert.Equal(t, val4, "")
	assert.False(t, ok4)
}

func makeSut() *NodeType {
	taints := []v1.Taint{
		{Key: "taint1", Value: "value1", Effect: v1.TaintEffectNoSchedule},
		{Key: "not-indexed-taint", Value: "not-indexed-taint-value", Effect: v1.TaintEffectNoSchedule},
		{Key: "taint2", Value: "value2", Effect: v1.TaintEffectNoSchedule},
	}

	labels := map[string]string{
		"label1":             "value1",
		"label2":             "value2",
		"not-indexed-label;": "not-indexed-label-value",
	}

	return NewNodeType(
		taints,
		labels,
		map[string]interface{}{"taint1": true, "taint2": true, "taint3": true},
		map[string]interface{}{"label1": true, "label2": true, "label3": true},
	)
}
