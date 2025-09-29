package failedpodchecks

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

func TestFailedContainerStatusChecker_IsRetryable(t *testing.T) {
	tests := map[string]struct {
		input          *v1.Pod
		checks         []podchecks.FailedContainerStatusCheck
		expectedResult bool
		expectMessage  bool
	}{
		"empty checks": {
			input:          makePodInitContainerWithNameAndMessage("orange", "message"),
			expectedResult: false,
			expectMessage:  false,
		},
		"matches on name regex": {
			input:          makePodContainerWithNameAndMessage("apple", ""),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "app.*"}},
			expectedResult: true,
			expectMessage:  false,
		},
		"matches on message regex": {
			input:          makePodInitContainerWithNameAndMessage("apple", "message"),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: ".*", MessageRegexp: "mess.*"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on name and message if supplied": {
			input:          makePodContainerWithNameAndMessage("banana", "message"),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "ban.*", MessageRegexp: "mess.*"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on name - no match": {
			input:          makePodInitContainerWithNameAndMessage("plum", "message"),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "app.*", MessageRegexp: ""}},
			expectedResult: false,
			expectMessage:  false,
		},
		"matches on message- no match": {
			input:          makePodContainerWithNameAndMessage("plum", "message"),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "plum", MessageRegexp: "memo.*"}},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - no match": {
			input: makePodInitContainerWithNameAndMessage("name", "message"),
			checks: []podchecks.FailedContainerStatusCheck{
				{ContainerNameRegexp: "nomenclature.*", MessageRegexp: ""},
				{ContainerNameRegexp: "name", MessageRegexp: "memo.*"},
				{ContainerNameRegexp: "nomenclature.*", MessageRegexp: "message2"},
			},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - match": {
			input: makePodContainerWithNameAndMessage("name", "message"),
			checks: []podchecks.FailedContainerStatusCheck{
				{ContainerNameRegexp: ".*", MessageRegexp: ""},
				{ContainerNameRegexp: "na.*", MessageRegexp: "message"},
				{ContainerNameRegexp: "mess.*", MessageRegexp: ""},
			},
			expectedResult: true,
			expectMessage:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			checker, err := NewFailedContainerStatusChecker(tc.checks)
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

func TestFailedContainerStatusChecker_Initialisation(t *testing.T) {
	// Empty
	_, err := NewFailedContainerStatusChecker([]podchecks.FailedContainerStatusCheck{})
	assert.NoError(t, err)
	// Valid
	_, err = NewFailedContainerStatusChecker([]podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: ".*"}})
	assert.NoError(t, err)
	// Invalid regex
	_, err = NewFailedContainerStatusChecker([]podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: ".*", MessageRegexp: "["}})
	assert.Error(t, err)
}

func makePodInitContainerWithNameAndMessage(name string, message string) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			InitContainerStatuses: []v1.ContainerStatus{
				{
					Name: name,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							Message: message,
						},
					},
				},
			},
		},
	}
}

func makePodContainerWithNameAndMessage(name string, message string) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: name,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							Message: message,
						},
					},
				},
			},
		},
	}
}
