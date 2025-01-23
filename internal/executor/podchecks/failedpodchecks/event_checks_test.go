package failedpodchecks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

func TestPodEventsChecker_IsRetryable(t *testing.T) {
	tests := map[string]struct {
		events         []*v1.Event
		checks         []podchecks.PodEventCheck
		expectedResult bool
		expectMessage  bool
	}{
		"empty checks": {
			events:         []*v1.Event{{Message: "Failed to allocate", Reason: "reason", Type: "Warning"}},
			expectedResult: false,
			expectMessage:  false,
		},
		"matches message and type": {
			events:         []*v1.Event{{Message: "Failed to allocate", Reason: "reason", Type: "Warning"}},
			checks:         []podchecks.PodEventCheck{{Regexp: "Fail.*", Type: "Warning"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on reason if supplied": {
			events:         []*v1.Event{{Message: "Failed to allocate", Reason: "reason", Type: "Warning"}},
			checks:         []podchecks.PodEventCheck{{Regexp: "Fail.*", Reason: "reason", Type: "Warning"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on reason if supplied - no match": {
			events:         []*v1.Event{{Message: "Failed to allocate", Reason: "reason", Type: "Warning"}},
			checks:         []podchecks.PodEventCheck{{Regexp: "Fail.*", Reason: "reason2", Type: "Warning"}},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple events and checks": {
			events: []*v1.Event{
				{Message: "Failed to allocate", Reason: "reason", Type: "Warning"},
				{Message: "Image pull", Reason: "image", Type: "Normal"},
				{Message: "Image error", Reason: "image", Type: "Warning"},
			},
			checks: []podchecks.PodEventCheck{
				{Regexp: "no.*", Type: "Warning"},
				{Regexp: "Image.*", Reason: "reason", Type: "Warning"},
				{Regexp: "Failed.*", Type: "Warning"},
				{Regexp: "Image err.*", Reason: "image", Type: "Warning"},
			},
			expectedResult: true,
			expectMessage:  true,
		},
		"multiple events and checks - no match": {
			events: []*v1.Event{
				{Message: "Failed to allocate", Reason: "reason", Type: "Warning"},
				{Message: "Image pull", Reason: "image", Type: "Normal"},
				{Message: "Image error", Reason: "image", Type: "Warning"},
			},
			checks: []podchecks.PodEventCheck{
				{Regexp: "no.*", Type: "Warning"},
				{Regexp: "Image.*", Reason: "reason", Type: "Warning"},
				{Regexp: "Failed.*", Type: "Normal"},
				{Regexp: "Image err.*", Reason: "image", Type: "Normal"},
			},
			expectedResult: false,
			expectMessage:  false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			checker, err := NewPodEventsChecker(tc.checks)
			require.NoError(t, err)

			isRetryable, message := checker.IsRetryable(tc.events)

			assert.Equal(t, tc.expectedResult, isRetryable)
			if tc.expectMessage {
				assert.NotEmpty(t, message)
			} else {
				assert.Empty(t, message)
			}
		})
	}
}

func TestPodEventsChecker_Initialisation(t *testing.T) {
	// Empty
	_, err := NewPodEventsChecker([]podchecks.PodEventCheck{})
	assert.NoError(t, err)
	// Valid
	_, err = NewPodEventsChecker([]podchecks.PodEventCheck{{Regexp: ".*", Type: "Warning"}})
	assert.NoError(t, err)
	// Invalid regex
	_, err = NewPodEventsChecker([]podchecks.PodEventCheck{{Regexp: "[", Type: "Warning"}})
	assert.Error(t, err)
	// Invalid type
	_, err = NewPodEventsChecker([]podchecks.PodEventCheck{{Regexp: ".*", Type: "Invalid"}})
	assert.Error(t, err)
}
