package configuration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/types"
)

func TestMutate(t *testing.T) {
	tests := map[string]struct {
		input    *Configuration
		expected *Configuration
	}{
		"MaxSchedulingDuration": {
			input: &Configuration{
				MaxSchedulingDuration: time.Second * 10,
			},
			expected: &Configuration{
				MaxSchedulingDuration: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxSchedulingDuration: time.Second * 10,
				},
			},
		},
		"MaxSchedulingDuration - overwrites scheduling.MaxSchedulingDuration": {
			input: &Configuration{
				MaxSchedulingDuration: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxSchedulingDuration: time.Second * 5,
				},
			},
			expected: &Configuration{
				MaxSchedulingDuration: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxSchedulingDuration: time.Second * 10,
				},
			},
		},
		"MaxNewJobSchedulingDuration": {
			input: &Configuration{
				NewJobsSchedulingTimeout: time.Second * 10,
			},
			expected: &Configuration{
				NewJobsSchedulingTimeout: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxNewJobSchedulingDuration: time.Second * 10,
				},
			},
		},
		"MaxNewJobSchedulingDuration - overwrites scheduling.MaxNewJobSchedulingDuration": {
			input: &Configuration{
				NewJobsSchedulingTimeout: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxNewJobSchedulingDuration: time.Second * 7,
				},
			},
			expected: &Configuration{
				NewJobsSchedulingTimeout: time.Second * 10,
				Scheduling: SchedulingConfig{
					MaxNewJobSchedulingDuration: time.Second * 10,
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := tc.input.Mutate()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestValidate_SchedulingTimeoutConfig(t *testing.T) {
	tests := map[string]struct {
		config        func(c Configuration) Configuration
		expectSuccess bool
	}{
		"valid - empty": {
			config:        func(c Configuration) Configuration { return c },
			expectSuccess: true,
		},
		"valid - all set": {
			config: func(c Configuration) Configuration {
				c.Scheduling.MaxSchedulingDuration = time.Second * 10
				c.Scheduling.MaxNewJobSchedulingDuration = time.Second * 5
				c.Scheduling.MaxNewJobSchedulingDurationPerQueue = time.Second
				return c
			},
			expectSuccess: true,
		},
		"valid - no max new job duration config": {
			config: func(c Configuration) Configuration {
				c.Scheduling.MaxSchedulingDuration = time.Second * 10
				return c
			},
			expectSuccess: true,
		},
		"invalid - MaxNewJobSchedulingDuration greater than MaxSchedulingDuration": {
			config: func(c Configuration) Configuration {
				c.Scheduling.MaxSchedulingDuration = time.Second * 10
				c.Scheduling.MaxNewJobSchedulingDuration = time.Second * 11
				return c
			},
			expectSuccess: false,
		},
		"invalid - MaxNewJobSchedulingDurationPerQueue greater than MaxSchedulingDuration": {
			config: func(c Configuration) Configuration {
				c.Scheduling.MaxSchedulingDuration = time.Second * 10
				c.Scheduling.MaxNewJobSchedulingDurationPerQueue = time.Second * 11
				return c
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			config := createValidMinimalConfig()
			config = tc.config(config)
			err := config.Validate()

			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func createValidMinimalConfig() Configuration {
	return Configuration{
		ExecutorTimeout:          time.Second,
		CyclePeriod:              time.Second,
		SchedulePeriod:           time.Second,
		DatabaseFetchSize:        1,
		QueueRefreshPeriod:       time.Second,
		InternedStringsCacheSize: 1,
		Http: HttpConfig{
			Port: 1,
		},
		Scheduling: SchedulingConfig{
			NodeIdLabel:                    "nodeid",
			MaximumSchedulingRate:          1,
			MaximumSchedulingBurst:         1,
			MaximumPerQueueSchedulingRate:  1,
			MaximumPerQueueSchedulingBurst: 1,
			MaxSchedulingDuration:          time.Second,
		},
		Leader: LeaderConfig{
			Mode: "local",
		},
		Pulsar: commonconfig.PulsarConfig{
			URL: "pulsar",
		},
	}
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
							Priority:              50,
							WellKnownNodeTypeName: "not-gpu",
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
