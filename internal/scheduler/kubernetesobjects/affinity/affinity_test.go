package affinity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

func TestAddNodeAntiAffinity_WhenAffinityNil_ReturnsError(t *testing.T) {
	var affinity *v1.Affinity = nil
	err := AddNodeAntiAffinity(affinity, "a", "b")
	assert.Error(t, err)
}

func TestAddNodeAntiAffinity_WhenNoExistingAffinity_AddsCorrectly(t *testing.T) {
	affinity := &v1.Affinity{}
	expected := vanillaAvoidLabelAffinity("a", "b")

	err := AddNodeAntiAffinity(affinity, "a", "b")
	assert.NoError(t, err)
	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenAlreadyThere_DoesNothing(t *testing.T) {
	affinity := vanillaAvoidLabelAffinity("a", "b")
	expected := vanillaAvoidLabelAffinity("a", "b")

	err := AddNodeAntiAffinity(affinity, "a", "b")
	assert.NoError(t, err)
	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenSameLabelDifferentValueAlreadyThere_IncludesBothValues(t *testing.T) {
	affinity := &v1.Affinity{}
	expected := vanillaAvoidLabelAffinity("a", "b")
	expected.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"b", "c"}

	err := AddNodeAntiAffinity(affinity, "a", "b")
	assert.NoError(t, err)
	err = AddNodeAntiAffinity(affinity, "a", "c")
	assert.NoError(t, err)
	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenDifferentLabelAlreadyThere_IncludesBothLabels(t *testing.T) {
	affinity := &v1.Affinity{}
	expected := vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: "a", Value: "b"}, {Key: "aa", Value: "bb"}})

	err := AddNodeAntiAffinity(affinity, "a", "b")
	assert.NoError(t, err)
	err = AddNodeAntiAffinity(affinity, "aa", "bb")
	assert.NoError(t, err)

	assert.Equal(t, expected, affinity)
}

func vanillaAvoidLabelAffinity(key string, val string) *v1.Affinity {
	return vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: key, Value: val}})
}

func vanillaAvoidLabelAffinites(labels []*api.StringKeyValuePair) *v1.Affinity {
	var mexprs []v1.NodeSelectorRequirement

	for _, kv := range labels {
		mexprs = append(mexprs, v1.NodeSelectorRequirement{
			Key:      kv.Key,
			Operator: v1.NodeSelectorOpNotIn,
			Values:   []string{kv.Value},
		})
	}

	return &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: mexprs,
					},
				},
			},
		},
	}
}
