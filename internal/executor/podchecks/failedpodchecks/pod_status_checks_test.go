package failedpodchecks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

func TestPodStatusChecker_IsRetryable(t *testing.T) {
	tests := map[string]struct {
		input          *v1.Pod
		checks         []podchecks.PodStatusCheck
		expectedResult bool
		expectMessage  bool
	}{
		"empty checks": {
			input:          makePodWithMessageAndReason("message", "reason"),
			expectedResult: false,
			expectMessage:  false,
		},
		"matches message on regex": {
			input:          makePodWithMessageAndReason("message", "reason"),
			checks:         []podchecks.PodStatusCheck{{Regexp: "mess.*"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on reason if supplied": {
			input:          makePodWithMessageAndReason("message", "reason"),
			checks:         []podchecks.PodStatusCheck{{Regexp: "mess.*", Reason: "reason"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on reason if supplied - no match": {
			input:          makePodWithMessageAndReason("message", "reason"),
			checks:         []podchecks.PodStatusCheck{{Regexp: "mess.*", Reason: "reason2"}},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - no match": {
			input: makePodWithMessageAndReason("message", "reason"),
			checks: []podchecks.PodStatusCheck{
				{Regexp: "reas.*", Reason: ""},
				{Regexp: "reas.*", Reason: "reason"},
				{Regexp: "mess.*", Reason: "reason2"},
			},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - match": {
			input: makePodWithMessageAndReason("message", "reason"),
			checks: []podchecks.PodStatusCheck{
				{Regexp: "reas.*", Reason: ""},
				{Regexp: "reas.*", Reason: "reason"},
				{Regexp: "mess.*", Reason: ""},
			},
			expectedResult: true,
			expectMessage:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			checker, err := NewPodStatusChecker(tc.checks)
			require.NoError(t, err)

			isRetryable, message := checker.IsRetryable(tc.input)

			assert.Equal(t, tc.expectedResult, isRetryable)
			if tc.expectMessage {
				assert.NotEmpty(t, message)
			} else {
				assert.Empty(t, message)
			}
		})
	}
}

func TestPodStatusChecker_Initialisation(t *testing.T) {
	// Empty
	_, err := NewPodStatusChecker([]podchecks.PodStatusCheck{})
	assert.NoError(t, err)
	// Valid
	_, err = NewPodStatusChecker([]podchecks.PodStatusCheck{{Regexp: ".*"}})
	assert.NoError(t, err)
	// Invalid regex
	_, err = NewPodStatusChecker([]podchecks.PodStatusCheck{{Regexp: "["}})
	assert.Error(t, err)
}

func makePodWithMessageAndReason(message string, reason string) *v1.Pod {
	return &v1.Pod{Status: v1.PodStatus{
		Message: message,
		Reason:  reason,
	}}
}
