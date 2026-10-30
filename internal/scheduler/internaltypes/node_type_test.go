package internaltypes

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		map[string]bool{"taint1": true, "taint2": true, "taint3": true},
		map[string]bool{"label1": true, "label2": true, "label3": true},
	)
}

func TestNodeTypeIdFromTaintsAndLabels_NoCollisions(t *testing.T) {
	taintKeys := []string{
		"node.kubernetes.io/not-ready",
		"node.kubernetes.io/unreachable",
		"nvidia.com/gpu",
		"armadaproject.io/reserved",
	}
	taintValues := []string{"", "true", "a100", "team-a"}
	taintEffects := []v1.TaintEffect{v1.TaintEffectNoSchedule, v1.TaintEffectPreferNoSchedule, v1.TaintEffectNoExecute}
	labelKeys := []string{"kubernetes.io/arch", "topology.kubernetes.io/zone", "armadaproject.io/pool"}
	labelValues := []string{"", "amd64", "arm64", "eu-west-1a", "cpu"}

	var allTaints []v1.Taint
	for _, key := range taintKeys {
		for _, value := range taintValues {
			for _, effect := range taintEffects {
				allTaints = append(allTaints, v1.Taint{Key: key, Value: value, Effect: effect})
			}
		}
	}
	// Taint sets of size zero, one and two.
	taintSets := [][]v1.Taint{{}}
	for i := range allTaints {
		taintSets = append(taintSets, []v1.Taint{allTaints[i]})
		for j := i + 1; j < len(allTaints); j++ {
			taintSets = append(taintSets, []v1.Taint{allTaints[i], allTaints[j]})
		}
	}
	// Every label key is either set to one of the values, indexed but unset, or absent.
	type labelSet struct{ labels, unset map[string]string }
	labelSets := []labelSet{{map[string]string{}, map[string]string{}}}
	for _, key := range labelKeys {
		var next []labelSet
		for _, ls := range labelSets {
			next = append(next, ls)
			for _, value := range labelValues {
				labels := copyStringMap(ls.labels)
				labels[key] = value
				next = append(next, labelSet{labels, ls.unset})
			}
			unset := copyStringMap(ls.unset)
			unset[key] = ""
			next = append(next, labelSet{ls.labels, unset})
		}
		labelSets = next
	}

	seen := make(map[uint64]string)
	for _, taints := range taintSets {
		for _, ls := range labelSets {
			input := describeNodeTypeInput(taints, ls.labels, ls.unset)
			id := nodeTypeIdFromTaintsAndLabels(taints, ls.labels, ls.unset)
			// The hashed string always contains the group separators, so the id is never that of an empty string.
			require.NotEqual(t, fnv1a.Init64, id, "id of %q is the hash of an empty string", input)
			if other, ok := seen[id]; ok {
				require.Failf(t, "node type id collision", "%q and %q both hash to %d", other, input, id)
			}
			seen[id] = input
		}
	}
	require.Greater(t, len(seen), 100000)
}

func TestNewNodeType_IdIndependentOfTaintOrder(t *testing.T) {
	// Kubernetes allows several taints with the same key as long as their effects differ.
	taints := []v1.Taint{
		{Key: "nvidia.com/gpu", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "nvidia.com/gpu", Value: "true", Effect: v1.TaintEffectNoExecute},
		{Key: "armadaproject.io/reserved", Value: "team-a", Effect: v1.TaintEffectNoSchedule},
	}
	want := NewNodeType(taints, nil, nil, nil).GetId()
	for _, perm := range [][]int{{0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}} {
		permuted := make([]v1.Taint, len(taints))
		for i, j := range perm {
			permuted[i] = taints[j]
		}
		assert.Equal(t, want, NewNodeType(permuted, nil, nil, nil).GetId(), "taint order %v", perm)
	}
}

func copyStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func describeNodeTypeInput(taints []v1.Taint, labels, unset map[string]string) string {
	parts := make([]string, 0, len(taints)+len(labels)+len(unset))
	for _, taint := range taints {
		parts = append(parts, fmt.Sprintf("taint %s=%s:%s", taint.Key, taint.Value, taint.Effect))
	}
	var groups []string
	for k, v := range labels {
		groups = append(groups, fmt.Sprintf("label %s=%s", k, v))
	}
	for k := range unset {
		groups = append(groups, fmt.Sprintf("unset %s", k))
	}
	sort.Strings(groups)
	return strings.Join(append(parts, groups...), ", ")
}
