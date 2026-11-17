package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryPolicyNames(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		want        []string
	}{
		"no annotation gives nil": {
			annotations: map[string]string{"other": "value"},
			want:        nil,
		},
		"the order of the list stays": {
			annotations: map[string]string{RetryPoliciesAnnotation: "gpu-transient,team-default"},
			want:        []string{"gpu-transient", "team-default"},
		},
		"spaces around a name are removed": {
			annotations: map[string]string{RetryPoliciesAnnotation: " gpu-transient , team-default "},
			want:        []string{"gpu-transient", "team-default"},
		},
		"an empty entry stays, so that validation can reject it": {
			annotations: map[string]string{RetryPoliciesAnnotation: "gpu-transient,,team-default"},
			want:        []string{"gpu-transient", "", "team-default"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, RetryPolicyNames(tc.annotations))
		})
	}
}
