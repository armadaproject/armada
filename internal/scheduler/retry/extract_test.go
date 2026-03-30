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

func fiFromErr(err *armadaevents.Error) *armadaevents.FailureInfo {
	if err == nil {
		return nil
	}
	return err.GetFailureInfo()
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
		"from FailureInfo": {
			err: &armadaevents.Error{
				FailureInfo: &armadaevents.FailureInfo{ExitCode: 137},
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{ExitCode: 1}},
					},
				},
			},
			expected: 137,
		},
		"fallback to ContainerError": {
			err: &armadaevents.Error{
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{ExitCode: 42}},
					},
				},
			},
			expected: 42,
		},
		"nil FailureInfo with zero exit code falls back": {
			err: &armadaevents.Error{
				FailureInfo: &armadaevents.FailureInfo{ExitCode: 0},
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{ExitCode: 99}},
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
			assert.Equal(t, tc.expected, extractExitCode(tc.err, fiFromErr(tc.err)))
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
		"from FailureInfo": {
			err: &armadaevents.Error{
				FailureInfo: &armadaevents.FailureInfo{TerminationMessage: "CUDA error"},
				Reason: &armadaevents.Error_PodError{
					PodError: &armadaevents.PodError{
						ContainerErrors: []*armadaevents.ContainerError{{Message: "container msg"}},
					},
				},
			},
			expected: "CUDA error",
		},
		"fallback to ContainerError": {
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
			assert.Equal(t, tc.expected, extractTerminationMessage(tc.err, fiFromErr(tc.err)))
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
		"from FailureInfo": {
			err: &armadaevents.Error{
				FailureInfo: &armadaevents.FailureInfo{Categories: []string{"gpu", "transient"}},
			},
			expected: []string{"gpu", "transient"},
		},
		"nil FailureInfo": {
			err:      &armadaevents.Error{},
			expected: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, extractCategories(fiFromErr(tc.err)))
		})
	}
}
