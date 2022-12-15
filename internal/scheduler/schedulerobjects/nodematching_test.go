package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeSchedulingRequirementsMet(t *testing.T) {
	tests := map[string]struct {
		Node          *Node
		Req           *PodRequirements
		ExpectSuccess bool
	}{
		"nil taints and labels": {
			Node: &Node{},
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: false,
		},
		"no taints or labels": {
			Node: &Node{
				Taints: make([]v1.Taint, 0),
				Labels: make(map[string]string),
			},
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
			},
			ExpectSuccess: true,
		},
		"untolerated taints": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			Req:           &PodRequirements{},
			ExpectSuccess: false,
		},
		"matched node affinity": {
			Node: &Node{
				Taints: nil,
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: true,
		},
		"unmatched node affinity": {
			Node: &Node{},
			Req: &PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and matched node affinity": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: true,
		},
		"untolerated taints and matched node affinity": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and unmatched node affinity": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
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
				},
			},
			ExpectSuccess: false,
		},
		"matched node selector": {
			Node: &Node{
				Taints: nil,
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"unmatched node selector": {
			Node: &Node{},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and matched node selector": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"untolerated taints and matched node selector": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and unmatched node selector": {
			Node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"sufficient cpu": {
			Node: &Node{
				AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType{
					0: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
					},
				},
				TotalResources: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			Req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			ExpectSuccess: true,
		},
		"insufficient cpu": {
			Node: &Node{
				AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType{
					0: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("0"),
						},
					},
				},
				TotalResources: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("0"),
					},
				},
			},
			Req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			ExpectSuccess: false,
		},
		"sufficient cpu at priority": {
			Node: &Node{
				AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType{
					0: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("0"),
						},
					},
					1: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
					},
				},
				TotalResources: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			Req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 1,
			},
			ExpectSuccess: true,
		},
		"insufficient cpu at priority": {
			Node: &Node{
				AllocatableByPriorityAndResource: AllocatableByPriorityAndResourceType{
					0: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("0"),
						},
					},
					1: ResourceList{
						Resources: map[string]resource.Quantity{
							"cpu": resource.MustParse("1"),
						},
					},
				},
				TotalResources: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu": resource.MustParse("1"),
					},
				},
			},
			Req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			ExpectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			matches, _, reason, err := tc.Node.PodRequirementsMet(tc.Req)
			assert.NoError(t, err)
			if tc.ExpectSuccess { // TODO: Test score set correctly.
				assert.True(t, matches)
				assert.Nil(t, reason)
			} else {
				assert.False(t, matches)
				assert.NotNil(t, reason) // TODO: Test for specific reason.
			}
		})
	}
}

func TestNodeTypeSchedulingRequirementsMet(t *testing.T) {
	tests := map[string]struct {
		Taints        []v1.Taint
		Labels        map[string]string
		IndexedTaints map[string]interface{}
		IndexedLabels map[string]interface{}
		Req           *PodRequirements
		ExpectSuccess bool
	}{
		"nil taints and labels": {
			Taints: nil,
			Labels: nil,
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"no taints or labels": {
			Taints: make([]v1.Taint, 0),
			Labels: make(map[string]string),
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"tolerated taints": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: nil,
			Req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
			},
			ExpectSuccess: true,
		},
		"untolerated taints": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        nil,
			Req:           &PodRequirements{},
			ExpectSuccess: false,
		},
		"untolerated non-indexed taint": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        nil,
			IndexedTaints: make(map[string]interface{}),
			Req:           &PodRequirements{},
			ExpectSuccess: true,
		},
		"matched node selector": {
			Taints:        nil,
			Labels:        map[string]string{"bar": "bar"},
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"unset indexed label": {
			Taints:        nil,
			Labels:        nil,
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"different label value": {
			Taints:        nil,
			Labels:        map[string]string{"bar": "baz"},
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"missing label": {
			Taints: nil,
			Labels: nil,
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"tolerated taints and matched node selector": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        map[string]string{"bar": "bar"},
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: true,
		},
		"untolerated taints and matched node selector": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        map[string]string{"bar": "bar"},
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and different label value": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        map[string]string{"bar": "baz"},
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
		"tolerated taints and missing label": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        nil,
			IndexedLabels: map[string]interface{}{"bar": ""},
			Req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			ExpectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeType := NewNodeType(
				tc.Taints,
				tc.Labels,
				tc.IndexedTaints,
				tc.IndexedLabels,
			)
			matches, reason, err := nodeType.PodRequirementsMet(tc.Req)
			assert.NoError(t, err)
			if tc.ExpectSuccess {
				assert.True(t, matches)
				assert.Nil(t, reason)
			} else {
				assert.False(t, matches)
				assert.NotNil(t, reason) // TODO: Test for specific reason.
			}
		})
	}
}
