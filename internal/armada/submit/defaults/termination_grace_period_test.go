package defaults

import (
	"k8s.io/utils/pointer"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestTerminationGracePeriodProcessor(t *testing.T) {
	defaultConfig := configuration.SubmissionConfig{
		MinTerminationGracePeriod: 1 * time.Hour,
	}

	tests := map[string]struct {
		config   configuration.SubmissionConfig
		podSpec  *v1.PodSpec
		expected *v1.PodSpec
	}{
		"Don't Default When Specified": {
			config: defaultConfig,
			podSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(500),
			},
			expected: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(500),
			},
		},
		"Default When Missing": {
			config:  defaultConfig,
			podSpec: &v1.PodSpec{},
			expected: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(3600),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			p := terminationGracePeriodProcessor{
				minTerminationGracePeriod: tc.config.MinTerminationGracePeriod,
			}
			msg := submitMsgFromPodSpec(tc.podSpec)
			p.Apply(msg)
			assert.Equal(t, tc.expected, msg.GetMainObject().GetPodSpec().GetPodSpec())
		})
	}
}
