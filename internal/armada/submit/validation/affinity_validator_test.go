package validation

import (
	"testing"

	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestAffinityValidator(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"No affinity": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: true,
		},
		"valid affinity": {
			req: &api.JobSubmitRequestItem{
				PodSpec: podSpecFromNodeSelector(v1.NodeSelectorRequirement{
					Key:      "bar",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"bar"},
				}),
			},
			expectSuccess: true,
		},
		"invalid affinity": {
			req: &api.JobSubmitRequestItem{
				PodSpec: podSpecFromNodeSelector(v1.NodeSelectorRequirement{
					Key:      "/keys_cant_start_with_a_slash",
					Operator: v1.NodeSelectorOpIn,
					Values:   []string{"bar"},
				}),
			},
			expectSuccess: false,
		},
		"PreferredDuringSchedulingIgnoredDuringExecution not allowed": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
								{},
							},
						},
					},
				},
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := affinityValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func podSpecFromNodeSelector(requirement v1.NodeSelectorRequirement) *v1.PodSpec {
	return &v1.PodSpec{
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{requirement},
						},
					},
				},
				PreferredDuringSchedulingIgnoredDuringExecution: nil,
			},
		},
	}
}
