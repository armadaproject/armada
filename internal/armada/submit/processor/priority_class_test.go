package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestPriorityClassProcessor(t *testing.T) {
	tests := map[string]struct {
		defaultPriorityClass string
		podSpec              *v1.PodSpec
		expected             *v1.PodSpec
	}{
		"Default PriorityClassName When Not Specified": {
			defaultPriorityClass: "pc",
			podSpec:              &v1.PodSpec{},
			expected: &v1.PodSpec{
				PriorityClassName: "pc",
			},
		},
		"Don't Default PriorityClassName When Already Present": {
			defaultPriorityClass: "pc",
			podSpec: &v1.PodSpec{
				PriorityClassName: "pc2",
			},
			expected: &v1.PodSpec{
				PriorityClassName: "pc2",
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := priorityClassProcessor{
				defaultPriorityClass: tc.defaultPriorityClass,
			}
			p.Apply(submitMsgFromPodSpec(tc.podSpec))
			assert.Equal(t, tc.expected, tc.podSpec)
		})
	}
}
