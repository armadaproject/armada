package failedpodchecks

import (
	"testing"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

func TestPodRetryChecker(t *testing.T) {
	podEventChecker := &stubEventRetryChecker{isRetryable: true, message: "event"}
	failPodEventChecker := &stubEventRetryChecker{isRetryable: false, message: ""}

	podStatusChecker := &stubPodStatusRetryChecker{isRetryable: true, message: "status"}
	failPodStatusChecker := &stubPodStatusRetryChecker{isRetryable: false, message: ""}

	tests := map[string]struct {
		podStatusChecker podStatusRetryChecker
		podEventChecker  eventRetryChecker
		expectRetryable  bool
	}{
		"retryable when all checkers return retryable": {
			podStatusChecker: podStatusChecker,
			podEventChecker:  podEventChecker,
			expectRetryable:  true,
		},
		"retryable when status checker returns retryable": {
			podStatusChecker: podStatusChecker,
			podEventChecker:  failPodEventChecker,
			expectRetryable:  true,
		},
		"retryable when event checker returns retryable": {
			podStatusChecker: failPodStatusChecker,
			podEventChecker:  podEventChecker,
			expectRetryable:  true,
		},
		"not retryable when all checkers return not retryable": {
			podStatusChecker: failPodStatusChecker,
			podEventChecker:  failPodEventChecker,
			expectRetryable:  false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			checker := PodRetryChecker{
				podEventChecker:  tc.podEventChecker,
				podStatusChecker: tc.podStatusChecker,
			}

			isRetryable, message := checker.IsRetryable(&v1.Pod{}, []*v1.Event{})

			assert.Equal(t, tc.expectRetryable, isRetryable)
			if tc.expectRetryable {
				assert.NotEmpty(t, message)
			} else {
				assert.Empty(t, message)
			}
		})
	}
}

func TestPodRetryCheckerIsRetryable_ChecksPodHasNotStartedAnyContainers(t *testing.T) {
	tests := map[string]struct {
		pod             *v1.Pod
		expectRetryable bool
	}{
		"pod with no container statuses - retryable": {
			pod:             &v1.Pod{},
			expectRetryable: true,
		},
		"pod with container status - not retryable": {
			pod: &v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							LastTerminationState: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
						},
					},
				},
			},
			expectRetryable: false,
		},
		"pod with init container status - not retryable": {
			pod: &v1.Pod{
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							LastTerminationState: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
						},
					},
				},
			},
			expectRetryable: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			checker := PodRetryChecker{
				podEventChecker:  &stubEventRetryChecker{isRetryable: true, message: "fallthrough"},
				podStatusChecker: &stubPodStatusRetryChecker{isRetryable: true, message: "fallthrough"},
			}

			isRetryable, message := checker.IsRetryable(tc.pod, []*v1.Event{})

			if tc.expectRetryable {
				assert.True(t, isRetryable)
				assert.Equal(t, "fallthrough", message)
			} else {
				assert.False(t, isRetryable)
				assert.Empty(t, message)
			}
		})
	}
}

func TestPodRetryChecker_SimpleEndToEnd(t *testing.T) {
	checker, err := NewPodRetryChecker(podchecks.FailedChecks{
		PodStatuses: []podchecks.PodStatusCheck{{Regexp: "mess.*", Reason: "reason"}},
		Events:      []podchecks.PodEventCheck{{Regexp: "Fail.*", Reason: "reason", Type: "Warning"}},
	})
	require.NoError(t, err)

	isRetryable, message := checker.IsRetryable(&v1.Pod{}, []*v1.Event{})
	assert.False(t, isRetryable)
	assert.Empty(t, message)

	// Pod matches status checker
	isRetryable, message = checker.IsRetryable(makePodWithMessageAndReason("message", "reason"), []*v1.Event{})
	assert.True(t, isRetryable)
	assert.NotEmpty(t, message)

	// Pod matches event checker
	isRetryable, message = checker.IsRetryable(&v1.Pod{}, []*v1.Event{{Message: "Failed to allocate", Reason: "reason", Type: "Warning"}})
	assert.True(t, isRetryable)
	assert.NotEmpty(t, message)
}

func TestPodRetryChecker_InitialiseEmpty(t *testing.T) {
	checker, err := NewPodRetryChecker(podchecks.FailedChecks{})
	require.NoError(t, err)

	isRetryable, message := checker.IsRetryable(&v1.Pod{}, []*v1.Event{})
	assert.False(t, isRetryable)
	assert.Empty(t, message)
}

type stubEventRetryChecker struct {
	isRetryable bool
	message     string
}

func (s *stubEventRetryChecker) IsRetryable(podEvents []*v1.Event) (bool, string) {
	return s.isRetryable, s.message
}

type stubPodStatusRetryChecker struct {
	isRetryable bool
	message     string
}

func (s *stubPodStatusRetryChecker) IsRetryable(pod *v1.Pod) (bool, string) {
	return s.isRetryable, s.message
}
