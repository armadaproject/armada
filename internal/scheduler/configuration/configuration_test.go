package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
)

func TestSchedulingConfigValidate(t *testing.T) {
	c := Configuration{
		Scheduling: configuration.SchedulingConfig{
			WellKnownNodeTypes: []configuration.WellKnownNodeType{
				{
					Name:   "gpu",
					Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}},
				},
				{
					Name:   "gpu",
					Taints: []v1.Taint{{Key: "other-key", Value: "true", Effect: v1.TaintEffectNoSchedule}},
				},
			},
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
	}
	expected := []string{
		configuration.DuplicateWellKnownNodeTypeErrorMessage,
		configuration.AwayNodeTypesWithoutPreemptionErrorMessage,
		configuration.UnknownWellKnownNodeTypeErrorMessage,
	}

	err := c.Validate()

	assert.Error(t, err)
	s := err.Error()
	for _, expected := range expected {
		assert.Contains(t, s, expected)
	}
}
