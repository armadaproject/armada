package schedulerobjects

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeSchedulingRequirementsMet(t *testing.T) {
	tests := map[string]struct {
		node          *Node
		req           *PodRequirements
		expectSuccess bool
	}{
		"nil taints and labels": {
			node: &Node{},
			req: &PodRequirements{
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
			expectSuccess: false,
		},
		"no taints or labels": {
			node: &Node{
				Taints: make([]v1.Taint, 0),
				Labels: make(map[string]string),
			},
			req: &PodRequirements{
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
			expectSuccess: false,
		},
		"tolerated taints": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			req: &PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
			},
			expectSuccess: true,
		},
		"untolerated taints": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			req:           &PodRequirements{},
			expectSuccess: false,
		},
		"matched node affinity": {
			node: &Node{
				Taints: nil,
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
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
			expectSuccess: true,
		},
		"unmatched node affinity": {
			node: &Node{},
			req: &PodRequirements{
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
			expectSuccess: false,
		},
		"tolerated taints and matched node affinity": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
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
			expectSuccess: true,
		},
		"untolerated taints and matched node affinity": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
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
			expectSuccess: false,
		},
		"tolerated taints and unmatched node affinity": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			req: &PodRequirements{
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
			expectSuccess: false,
		},
		"matched node selector": {
			node: &Node{
				Taints: nil,
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			expectSuccess: true,
		},
		"unmatched node selector": {
			node: &Node{},
			req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			expectSuccess: false,
		},
		"tolerated taints and matched node selector": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			expectSuccess: true,
		},
		"untolerated taints and matched node selector": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: map[string]string{"bar": "bar"},
			},
			req: &PodRequirements{
				NodeSelector: map[string]string{"bar": "bar"},
			},
			expectSuccess: false,
		},
		"tolerated taints and unmatched node selector": {
			node: &Node{
				Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
				Labels: nil,
			},
			req: &PodRequirements{
				Tolerations:  []v1.Toleration{{Key: "foo", Value: "foo"}},
				NodeSelector: map[string]string{"bar": "bar"},
			},
			expectSuccess: false,
		},
		"sufficient cpu": {
			node: &Node{
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
			req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			expectSuccess: true,
		},
		"insufficient cpu": {
			node: &Node{
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
			req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			expectSuccess: false,
		},
		"sufficient cpu at priority": {
			node: &Node{
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
			req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 1,
			},
			expectSuccess: true,
		},
		"insufficient cpu at priority": {
			node: &Node{
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
			req: &PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu": resource.MustParse("1"),
					},
				},
				Priority: 0,
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			matches, _, reason, err := PodRequirementsMet(tc.node.Taints, tc.node.Labels, tc.node.TotalResources, tc.node.AllocatableByPriorityAndResource[tc.req.Priority], tc.req)
			assert.NoError(t, err)
			if tc.expectSuccess { // TODO: Test score set correctly.
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
			matches, reason, err := NodeTypePodRequirementsMet(nodeType, tc.Req)
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

func TestInsufficientResourcesSum64(t *testing.T) {
	tests := map[string]struct {
		a     *InsufficientResources
		b     *InsufficientResources
		equal bool
	}{
		"different resource": {
			a: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("2"),
				Available: resource.MustParse("1"),
			},
			b: &InsufficientResources{
				Resource:  "bar",
				Required:  resource.MustParse("2"),
				Available: resource.MustParse("1"),
			},
			equal: false,
		},
		"different required": {
			a: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("2"),
				Available: resource.MustParse("1"),
			},
			b: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("3"),
				Available: resource.MustParse("1"),
			},
			equal: false,
		},
		"different available": {
			a: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("2"),
				Available: resource.MustParse("1"),
			},
			b: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("2"),
				Available: resource.MustParse("2"),
			},
			equal: false,
		},
		"small difference": {
			a: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("1000m"),
				Available: resource.MustParse("1"),
			},
			b: &InsufficientResources{
				Resource:  "foo",
				Required:  resource.MustParse("1001m"),
				Available: resource.MustParse("1"),
			},
			equal: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.equal {
				assert.Equal(t, tc.a.Sum64(), tc.b.Sum64())
			} else {
				assert.NotEqual(t, tc.a.Sum64(), tc.b.Sum64())
			}
		})
	}
}

func BenchmarkUntoleratedTaintSum64(b *testing.B) {
	reason := &UntoleratedTaint{
		Taint: v1.Taint{Key: randomString(100), Value: randomString(100), Effect: v1.TaintEffectNoSchedule},
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reason.Sum64()
	}
}

func BenchmarkInsufficientResourcesSum64(b *testing.B) {
	reason := &InsufficientResources{
		Resource:  randomString(100),
		Required:  resource.MustParse("2"),
		Available: resource.MustParse("1"),
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reason.Sum64()
	}
}

func randomString(n int) string {
	s := ""
	for i := 0; i < n; i++ {
		s += fmt.Sprint(i)
	}
	return s
}
