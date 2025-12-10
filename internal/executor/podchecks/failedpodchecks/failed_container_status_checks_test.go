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
			input:          makePodInitContainerWithNameMessageCode("orange", "message", 1),
			expectedResult: false,
			expectMessage:  false,
		},
		"matches on name regex": {
			input:          makePodContainerWithNameMessageCode("apple", "", 1),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "app.*"}},
			expectedResult: true,
			expectMessage:  false,
		},
		"matches on message regex": {
			input:          makePodInitContainerWithNameMessageCode("apple", "message", 137),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: ".*", MessageRegexp: "mess.*"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on name and message if supplied": {
			input:          makePodContainerWithNameMessageCode("banana", "message", 1),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "ban.*", MessageRegexp: "mess.*"}},
			expectedResult: true,
			expectMessage:  true,
		},
		"matches on name - no match": {
			input:          makePodInitContainerWithNameMessageCode("plum", "message", 1),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "app.*", MessageRegexp: ""}},
			expectedResult: false,
			expectMessage:  false,
		},
		"matches on message- no match": {
			input:          makePodContainerWithNameMessageCode("plum", "message", 1),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: "plum", MessageRegexp: "memo.*"}},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - no match": {
			input: makePodInitContainerWithNameMessageCode("name", "message", 1),
			checks: []podchecks.FailedContainerStatusCheck{
				{ContainerNameRegexp: "nomenclature.*", MessageRegexp: ""},
				{ContainerNameRegexp: "name", MessageRegexp: "memo.*"},
				{ContainerNameRegexp: "nomenclature.*", MessageRegexp: "message2"},
			},
			expectedResult: false,
			expectMessage:  false,
		},
		"multiple checks - match": {
			input: makePodContainerWithNameMessageCode("name", "message", 1),
			checks: []podchecks.FailedContainerStatusCheck{
				{ContainerNameRegexp: ".*", MessageRegexp: ""},
				{ContainerNameRegexp: "na.*", MessageRegexp: "message"},
				{ContainerNameRegexp: "mess.*", MessageRegexp: ""},
			},
			expectedResult: true,
			expectMessage:  true,
		},
		"terminated status zero - no match": {
			input:          makePodInitContainerWithNameMessageCode("apple", "message", 0),
			checks:         []podchecks.FailedContainerStatusCheck{{ContainerNameRegexp: ".*", MessageRegexp: "mess.*"}},
			expectedResult: false,
			expectMessage:  false,
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

func makePodInitContainerWithNameMessageCode(name string, message string, code int32) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			InitContainerStatuses: []v1.ContainerStatus{
				{
					Name: name,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							Message:  message,
							ExitCode: code,
						},
					},
				},
			},
		},
	}
}

func makePodContainerWithNameMessageCode(name string, message string, code int32) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name: name,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{
							Message:  message,
							ExitCode: code,
						},
					},
				},
			},
		},
	}
}
