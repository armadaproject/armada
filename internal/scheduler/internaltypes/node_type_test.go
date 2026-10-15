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

func TestNodeTypeIdFromTaintsAndLabels_NoCollisions(t *testing.T) {
	taintKeys := []string{
		"node.kubernetes.io/not-ready",
		"node.kubernetes.io/unreachable",
		"node.kubernetes.io/disk-pressure",
		"node.kubernetes.io/memory-pressure",
		"node.kubernetes.io/pid-pressure",
		"node.kubernetes.io/unschedulable",
		"node.cloudprovider.kubernetes.io/shutdown",
	}
	taintEffects := []v1.TaintEffect{
		v1.TaintEffectNoSchedule,
		v1.TaintEffectPreferNoSchedule,
		v1.TaintEffectNoExecute,
	}
	labelKeys := []string{
		"kubernetes.io/arch",
		"kubernetes.io/os",
		"topology.kubernetes.io/zone",
		"topology.kubernetes.io/region",
		"node.kubernetes.io/instance-type",
	}
	labelValues := []string{"amd64", "arm64", "linux", "us-east-1a", "us-west-2b", "m5.xlarge", "c5.2xlarge"}

	type hashInput struct {
		taints             []v1.Taint
		labels             map[string]string
		unsetIndexedLabels map[string]string
	}

	seen := make(map[uint64]hashInput)
	collisions := 0

	// Generate combinations: each taint key x effect as a single-taint node type
	for _, key := range taintKeys {
		for _, effect := range taintEffects {
			taints := []v1.Taint{{Key: key, Value: "true", Effect: effect}}
			h := nodeTypeIdFromTaintsAndLabels(taints, nil, nil)
			assert.NotEqual(t, uint64(0), h, "hash should not be zero for taints=%v", taints)
			input := hashInput{taints: taints}
			if prev, exists := seen[h]; exists {
				t.Errorf("hash collision: %v and %v both produce %d", prev, input, h)
				collisions++
			}
			seen[h] = input
		}
	}

	// Generate combinations: each label key x value as a single-label node type
	for _, key := range labelKeys {
		for _, value := range labelValues {
			labels := map[string]string{key: value}
			h := nodeTypeIdFromTaintsAndLabels(nil, labels, nil)
			assert.NotEqual(t, uint64(0), h, "hash should not be zero for labels=%v", labels)
			input := hashInput{labels: labels}
			if prev, exists := seen[h]; exists {
				t.Errorf("hash collision: %v and %v both produce %d", prev, input, h)
				collisions++
			}
			seen[h] = input
		}
	}

	// Generate combinations: unset indexed labels
	for _, key := range labelKeys {
		unset := map[string]string{key: ""}
		h := nodeTypeIdFromTaintsAndLabels(nil, nil, unset)
		assert.NotEqual(t, uint64(0), h, "hash should not be zero for unsetLabels=%v", unset)
		input := hashInput{unsetIndexedLabels: unset}
		if prev, exists := seen[h]; exists {
			t.Errorf("hash collision: %v and %v both produce %d", prev, input, h)
			collisions++
		}
		seen[h] = input
	}

	// Mixed: taint + label combinations
	for _, tKey := range taintKeys[:3] {
		for _, lKey := range labelKeys[:3] {
			for _, lVal := range labelValues[:3] {
				taints := []v1.Taint{{Key: tKey, Value: "true", Effect: v1.TaintEffectNoSchedule}}
				labels := map[string]string{lKey: lVal}
				h := nodeTypeIdFromTaintsAndLabels(taints, labels, nil)
				input := hashInput{taints: taints, labels: labels}
				if prev, exists := seen[h]; exists {
					t.Errorf("hash collision: %v and %v both produce %d", prev, input, h)
					collisions++
				}
				seen[h] = input
			}
		}
	}

	t.Logf("tested %d unique inputs with %d collisions", len(seen), collisions)
}

func TestNodeTypeIdFromTaintsAndLabels_NeverEmpty(t *testing.T) {
	// Even with empty inputs, the hash should not be zero (FNV offset basis)
	h := nodeTypeIdFromTaintsAndLabels(nil, nil, nil)
	assert.NotEqual(t, uint64(0), h, "hash of empty inputs should not be zero")
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
		map[string]bool{"taint1": true, "taint2": true, "taint3": true},
		map[string]bool{"label1": true, "label2": true, "label3": true},
	)
}
