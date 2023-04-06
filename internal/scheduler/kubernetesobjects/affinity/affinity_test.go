package affinity

import (
	"testing"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestAddNodeAntiAffinity_WhenNoExistingAffinity_AddsCorrectly(t *testing.T) {
	affinity := &v1.Affinity{}
	AddNodeAntiAffinity(affinity, "a", "b")
	expected := vanillaAvoidLabelAffinity("a", "b")

	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenAlreadyThere_DoesNothing(t *testing.T) {
	affinity := vanillaAvoidLabelAffinity("a", "b")
	AddNodeAntiAffinity(affinity, "a", "b")
	expected := vanillaAvoidLabelAffinity("a", "b")

	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenSameLabelDifferentValueAlreadyThere_IncludesBothValues(t *testing.T) {
	affinity := &v1.Affinity{}
	AddNodeAntiAffinity(affinity, "a", "b")
	AddNodeAntiAffinity(affinity, "a", "c")

	expected := vanillaAvoidLabelAffinity("a", "b")
	expected.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"b", "c"}

	assert.Equal(t, expected, affinity)
}

func TestAddNodeAntiAffinity_WhenDifferentLabelAlreadyThere_IncludesBothLabels(t *testing.T) {
	affinity := &v1.Affinity{}
	AddNodeAntiAffinity(affinity, "a", "b")
	AddNodeAntiAffinity(affinity, "aa", "bb")

	expected := vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: "a", Value: "b"}, {Key: "aa", Value: "bb"}})

	assert.Equal(t, expected, affinity)
}

func vanillaAvoidLabelAffinity(key string, val string) *v1.Affinity {
	return vanillaAvoidLabelAffinites([]*api.StringKeyValuePair{{Key: key, Value: val}})
}

func vanillaAvoidLabelAffinites(labels []*api.StringKeyValuePair) *v1.Affinity {
	mexprs := []v1.NodeSelectorRequirement{}

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
