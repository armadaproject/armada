package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestCanSchedulePod(t *testing.T) {
	tests := map[string]struct {
		Taints        []v1.Taint
		Labels        map[string]string
		Req           *PodSchedulingRequirements
		ExpectSuccess bool
	}{
		"nil taints and labels": {
			Taints: nil,
			Labels: nil,
			Req: &PodSchedulingRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: false,
		},
		"no taints or labels": {
			Taints: make([]v1.Taint, 0),
			Labels: make(map[string]string),
			Req: &PodSchedulingRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: nil,
			Req: &PodSchedulingRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
			},
			ExpectSuccess: true,
		},
		"untolerated taints": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        nil,
			Req:           &PodSchedulingRequirements{},
			ExpectSuccess: false,
		},
		"matched labels": {
			Taints: nil,
			Labels: map[string]string{"bar": "bar"},
			Req: &PodSchedulingRequirements{
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: true,
		},
		"unmatched labels": {
			Taints: nil,
			Labels: nil,
			Req: &PodSchedulingRequirements{
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and matched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: map[string]string{"bar": "bar"},
			Req: &PodSchedulingRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: true,
		},
		"untolerated taints and matched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: map[string]string{"bar": "bar"},
			Req: &PodSchedulingRequirements{
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and unmatched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: nil,
			Req: &PodSchedulingRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "bar",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
			ExpectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeType := NewNodeType(tc.Taints, tc.Labels, nil, nil)
			err := nodeType.canSchedulePod(tc.Req)
			if tc.ExpectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
