package configuration

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/types"
)

func TestSchedulingConfigValidate(t *testing.T) {
	c := Configuration{
		Scheduling: SchedulingConfig{
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
			PriorityClasses: map[string]types.PriorityClass{
				"armada-preemptible-away": {
					Priority: 100,

					AwayNodeTypes: []types.AwayNodeType{
						{
							Priority:           50,
							WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{{Name: "not-gpu"}},
						},
					},
				},
			},
		},
	}
	expected := []string{
		DuplicateWellKnownNodeTypeErrorMessage,
		AwayNodeTypesWithoutPreemptionErrorMessage,
		UnknownWellKnownNodeTypeErrorMessage,
	}

	err := c.Validate()

	assert.Error(t, err)
	s := err.Error()
	for _, expected := range expected {
		assert.Contains(t, s, expected)
	}
}

// validateSchedulingConfig runs only the custom SchedulingConfig validation, avoiding
// the other required-field checks on the surrounding Configuration struct.
func validateSchedulingConfig(c SchedulingConfig) error {
	v := validator.New()
	v.RegisterStructValidation(SchedulingConfigValidation, SchedulingConfig{})
	return v.Struct(c)
}

func TestSchedulingConfigValidate_ConditionOperator(t *testing.T) {
	c := SchedulingConfig{
		WellKnownNodeTypes: []WellKnownNodeType{
			{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
		},
		PriorityClasses: map[string]types.PriorityClass{
			"armada-preemptible-away": {
				Priority:    100,
				Preemptible: true,
				AwayNodeTypes: []types.AwayNodeType{
					{
						Priority: 50,
						WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
							{
								Name: "gpu",
								Conditions: []types.AwayNodeTypeCondition{
									{Resource: "cpu", Operator: "!=", Value: "1"},
								},
							},
						},
					},
				},
			},
		},
	}

	err := validateSchedulingConfig(c)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), InvalidAwayNodeTypeConditionOperatorErrorMessage)
}

func TestSchedulingConfigValidate_ConditionValue(t *testing.T) {
	c := SchedulingConfig{
		WellKnownNodeTypes: []WellKnownNodeType{
			{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
		},
		PriorityClasses: map[string]types.PriorityClass{
			"armada-preemptible-away": {
				Priority:    100,
				Preemptible: true,
				AwayNodeTypes: []types.AwayNodeType{
					{
						Priority: 50,
						WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
							{
								Name: "gpu",
								Conditions: []types.AwayNodeTypeCondition{
									{Resource: "cpu", Operator: ">=", Value: "not-a-quantity"},
								},
							},
						},
					},
				},
			},
		},
	}

	err := validateSchedulingConfig(c)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), InvalidAwayNodeTypeConditionValueErrorMessage)
}

func TestSchedulingConfigValidate_ValidConditions(t *testing.T) {
	c := SchedulingConfig{
		WellKnownNodeTypes: []WellKnownNodeType{
			{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
		},
		PriorityClasses: map[string]types.PriorityClass{
			"armada-preemptible-away": {
				Priority:    100,
				Preemptible: true,
				AwayNodeTypes: []types.AwayNodeType{
					{
						Priority: 50,
						WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
							{
								Name: "gpu",
								Conditions: []types.AwayNodeTypeCondition{
									{Resource: "nvidia.com/gpu", Operator: ">=", Value: "1"},
									{Resource: "cpu", Operator: ">", Value: "4"},
								},
							},
						},
					},
				},
			},
		},
	}

	err := validateSchedulingConfig(c)
	assert.NoError(t, err)
}

func TestSchedulingConfigValidate(t *testing.T) {
	c := Configuration{
		Scheduling: SchedulingConfig{
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
			PriorityClasses: map[string]types.PriorityClass{
				"armada-preemptible-away": {
					Priority: 100,

					AwayNodeTypes: []types.AwayNodeType{
						{
							Priority:           50,
							WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{{Name: "not-gpu"}},
						},
					},
				},
			},
		},
	}
	expected := []string{
		DuplicateWellKnownNodeTypeErrorMessage,
		AwayNodeTypesWithoutPreemptionErrorMessage,
		UnknownWellKnownNodeTypeErrorMessage,
	}

	err := c.Validate()

	assert.Error(t, err)
	s := err.Error()
	for _, expected := range expected {
		assert.Contains(t, s, expected)
	}
}

func TestSchedulingConfigValidate_ConditionOperator(t *testing.T) {
	c := Configuration{
		Scheduling: SchedulingConfig{
			WellKnownNodeTypes: []WellKnownNodeType{
				{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
			},
			PriorityClasses: map[string]types.PriorityClass{
				"armada-preemptible-away": {
					Priority:    100,
					Preemptible: true,
					AwayNodeTypes: []types.AwayNodeType{
						{
							Priority: 50,
							WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
								{
									Name: "gpu",
									Conditions: []types.AwayNodeTypeCondition{
										{Resource: "cpu", Operator: "!=", Value: "1"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := c.Validate()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), InvalidAwayNodeTypeConditionOperatorErrorMessage)
}

func TestSchedulingConfigValidate_ConditionValue(t *testing.T) {
	c := Configuration{
		Scheduling: SchedulingConfig{
			WellKnownNodeTypes: []WellKnownNodeType{
				{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
			},
			PriorityClasses: map[string]types.PriorityClass{
				"armada-preemptible-away": {
					Priority:    100,
					Preemptible: true,
					AwayNodeTypes: []types.AwayNodeType{
						{
							Priority: 50,
							WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
								{
									Name: "gpu",
									Conditions: []types.AwayNodeTypeCondition{
										{Resource: "cpu", Operator: ">=", Value: "not-a-quantity"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := c.Validate()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), InvalidAwayNodeTypeConditionValueErrorMessage)
}

func TestSchedulingConfigValidate_ValidConditions(t *testing.T) {
	c := Configuration{
		Scheduling: SchedulingConfig{
			WellKnownNodeTypes: []WellKnownNodeType{
				{Name: "gpu", Taints: []v1.Taint{{Key: "gpu", Value: "true", Effect: v1.TaintEffectNoSchedule}}},
			},
			PriorityClasses: map[string]types.PriorityClass{
				"armada-preemptible-away": {
					Priority:    100,
					Preemptible: true,
					AwayNodeTypes: []types.AwayNodeType{
						{
							Priority: 50,
							WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
								{
									Name: "gpu",
									Conditions: []types.AwayNodeTypeCondition{
										{Resource: "nvidia.com/gpu", Operator: ">=", Value: "1"},
										{Resource: "cpu", Operator: ">", Value: "4"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := c.Validate()
	assert.NoError(t, err)
}
