package processor

import (
	"testing"

	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestPriorityClassProcessor(t *testing.T) {
	tests := map[string]struct {
		config   configuration.SchedulingConfig
		podSpec  v1.PodSpec
		expected v1.PodSpec
	}{
		"Default PriorityClassName When Not Specified": {
			config: configuration.SchedulingConfig{
				Preemption: configuration.PreemptionConfig{
					DefaultPriorityClass: "pc",
				},
			},
			podSpec: v1.PodSpec{},
			expected: v1.PodSpec{
				PriorityClassName: "pc",
			},
		},
		"Don't Default PriorityClassName When Already Present": {
			config: configuration.SchedulingConfig{
				Preemption: configuration.PreemptionConfig{
					DefaultPriorityClass: "pc1",
				},
			},
			podSpec: v1.PodSpec{
				PriorityClassName: "pc2",
			},
			expected: v1.PodSpec{
				PriorityClassName: "pc2",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := priorityClassProcessor{
				defaultPriorityClass: tc.config.Preemption.DefaultPriorityClass,
			}
			p.processPodSpec(&tc.podSpec)
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}
