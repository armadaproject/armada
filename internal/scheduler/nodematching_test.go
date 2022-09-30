package scheduler

import (
	"testing"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestPodRequirementsMet(t *testing.T) {
	tests := map[string]struct {
		Taints        []v1.Taint
		Labels        map[string]string
		Req           *schedulerobjects.PodRequirements
		ExpectSuccess bool
	}{
		"nil taints and labels": {
			Taints: nil,
			Labels: nil,
			Req: &schedulerobjects.PodRequirements{
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
			Taints: make([]v1.Taint, 0),
			Labels: make(map[string]string),
			Req: &schedulerobjects.PodRequirements{
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
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: nil,
			Req: &schedulerobjects.PodRequirements{
				Tolerations: []v1.Toleration{{Key: "foo", Value: "foo"}},
			},
			ExpectSuccess: true,
		},
		"untolerated taints": {
			Taints:        []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels:        nil,
			Req:           &schedulerobjects.PodRequirements{},
			ExpectSuccess: false,
		},
		"matched labels": {
			Taints: nil,
			Labels: map[string]string{"bar": "bar"},
			Req: &schedulerobjects.PodRequirements{
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
		"unmatched labels": {
			Taints: nil,
			Labels: nil,
			Req: &schedulerobjects.PodRequirements{
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
		"tolerated taints and matched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: map[string]string{"bar": "bar"},
			Req: &schedulerobjects.PodRequirements{
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
		"untolerated taints and matched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: map[string]string{"bar": "bar"},
			Req: &schedulerobjects.PodRequirements{
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
		"tolerated taints and unmatched labels": {
			Taints: []v1.Taint{{Key: "foo", Value: "foo", Effect: v1.TaintEffectNoSchedule}},
			Labels: nil,
			Req: &schedulerobjects.PodRequirements{
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
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			matches, reason, err := PodRequirementsMet(tc.Taints, tc.Labels, tc.Req)
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
