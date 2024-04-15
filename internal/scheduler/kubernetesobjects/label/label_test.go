package label

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestMatchNodeSelectorTermsMatch(t *testing.T) {
	result, err := MatchNodeSelectorTerms(map[string]string{"key": "val"}, testNodeSelector())
	assert.True(t, result)
	assert.Nil(t, err)
}

func TestMatchNodeSelectorTermsNoMatch(t *testing.T) {
	result, err := MatchNodeSelectorTerms(map[string]string{"key": "missing-val"}, testNodeSelector())
	assert.False(t, result)
	assert.Nil(t, err)
}

func testNodeSelector() *v1.NodeSelector {
	return &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{{Key: "key", Operator: v1.NodeSelectorOpIn, Values: []string{"val"}}},
			},
		},
	}
}
