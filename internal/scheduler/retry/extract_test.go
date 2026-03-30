package retry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestExtractCondition(t *testing.T) {
	tests := map[string]struct {
		err      *armadaevents.Error
		expected string
	}{
		"nil error": {
			err:      nil,
			expected: "",
		},
		"PodError OOM": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_OOM},
				},
			},
			expected: errormatch.ConditionOOMKilled,
		},
		"PodError Evicted": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_Evicted},
				},
			},
			expected: errormatch.ConditionEvicted,
		},
		"PodError DeadlineExceeded": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_DeadlineExceeded},
				},
			},
			expected: errormatch.ConditionDeadlineExceeded,
		},
		"PodError AppError": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{KubernetesReason: armadaevents.KubernetesReason_AppError},
				},
			},
			expected: errormatch.ConditionAppError,
		},
		"PodError nil inner": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{PodError: nil},
			},
			expected: "",
		},
		"Preempted": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_JobRunPreemptedError{
					JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
				},
			},
			expected: errormatch.ConditionPreempted,
		},
		"LeaseReturned": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodLeaseReturned{
					PodLeaseReturned: &armadaevents.PodLeaseReturned{},
				},
			},
			expected: errormatch.ConditionLeaseReturned,
		},
		"unrecognized error type": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_LeaseExpired{
					LeaseExpired: &armadaevents.LeaseExpired{},
				},
			},
			expected: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractCondition(tc.err))
		})
	}
}

func TestExtractExitCode(t *testing.T) {
	tests := map[string]struct {
		err      *armadaevents.Error
		expected int32
	}{
		"nil error": {
			err:      nil,
			expected: 0,
		},
		"from ContainerError": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{ExitCode: 42}},
					},
				},
			},
			expected: 42,
		},
		"first non-zero ContainerError wins": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{
							{ExitCode: 0},
							{ExitCode: 99},
						},
					},
				},
			},
			expected: 99,
		},
		"no exit code anywhere": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{},
				},
			},
			expected: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractExitCode(tc.err))
		})
	}
}

func TestExtractTerminationMessage(t *testing.T) {
	tests := map[string]struct {
		err      *armadaevents.Error
		expected string
	}{
		"nil error": {
			err:      nil,
			expected: "",
		},
		"from ContainerError": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{Message: "segfault"}},
					},
				},
			},
			expected: "segfault",
		},
		"no message anywhere": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{},
				},
			},
			expected: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractTerminationMessage(tc.err))
		})
	}
}

func TestExtractCategories(t *testing.T) {
	tests := map[string]struct {
		err      *armadaevents.Error
		expected []string
	}{
		"nil error": {
			err:      nil,
			expected: nil,
		},
		"category and subcategory both present": {
			err: &armadaevents.Error{
				FailureCategory:    "infrastructure",
				FailureSubcategory: "oom",
			},
			expected: []string{"infrastructure", "oom"},
		},
		"only category present": {
			err: &armadaevents.Error{
				FailureCategory: "user_error",
			},
			expected: []string{"user_error"},
		},
		"only subcategory present": {
			err: &armadaevents.Error{
				FailureSubcategory: "signal_killed",
			},
			expected: []string{"signal_killed"},
		},
		"both empty": {
			err:      &armadaevents.Error{},
			expected: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractCategories(tc.err))
		})
	}
}
