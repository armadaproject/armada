package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/types"
)

func TestSchedulingConfigValidate(t *testing.T) {
	tests := map[string]struct {
		Config   SchedulingConfig
		Expected []string
	}{
		"valid": {
			Config: SchedulingConfig{
				WellKnownNodeTypes: []WellKnownNodeType{
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				},
				Preemption: PreemptionConfig{
					PriorityClasses: map[string]types.PriorityClass{
						"armada-preemptible-away": {
							Priority:    100,
							Preemptible: true,

							AwayNodeTypes: []types.AwayNodeType{
								{
									Priority:              50,
									WellKnownNodeTypeName: "gpu",
								},
							},
						},
					},
				},
			},
			Expected: nil,
		},
		"invalid": {
			Config: SchedulingConfig{
				WellKnownNodeTypes: []WellKnownNodeType{
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
					{
						Name:   "gpu",
						Taints: []v1.Taint{{Key: "other-key", Value: "true", Effect: v1.TaintEffectNoSchedule}},
					},
				},
				Preemption: PreemptionConfig{
					PriorityClasses: map[string]types.PriorityClass{
						"armada-preemptible-away": {
							Priority: 100,

							AwayNodeTypes: []types.AwayNodeType{
								{
									Priority:              50,
									WellKnownNodeTypeName: "not-gpu",
								},
							},
						},
					},
				},
			},
			Expected: []string{
				"duplicate well-known node type name gpu",
				"armada-preemptible-away has away node types but is not preemptible",
				"away node type not-gpu",
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.Config.Validate()
			if len(test.Expected) == 0 {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				s := err.Error()
				for _, expected := range test.Expected {
					assert.Contains(t, s, expected)
				}
			}
		})
	}
}
