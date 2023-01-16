package util

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/pkg/api"
)

var (
	evictedPod          *v1.Pod
	oomPod              *v1.Pod
	customErrorPod      *v1.Pod
	deadlineExceededPod *v1.Pod
)

func init() {
	evictedPod = createEvictedPod()
	oomPod = createFailedPod(createOomContainerStatus())
	customErrorPod = createFailedPod(createCustomErrorContainerStatus())
	deadlineExceededPod = createDeadlineExceededPod()
}

func TestHasUnstableContainerStates_ReturnFalse_WhenNoIssues(t *testing.T) {
	runningContainer := v1.ContainerState{
		Running: &v1.ContainerStateRunning{},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{runningContainer}, []v1.ContainerState{})
	result := hasUnstableContainerStates(pod)

	assert.False(t, result)
}

func TestHasUnstableContainerStates_ReturnTrue_WhenContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := hasUnstableContainerStates(pod)

	assert.True(t, result)
}

func TestHasUnstableContainerStates_ReturnTrue_WhenContainerInErrImagePull(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ErrImagePull",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := hasUnstableContainerStates(pod)

	assert.True(t, result)
}

func TestContainersAreRetryable_ReturnTrue_WhenInitContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{}, []v1.ContainerState{imagePullBackoffState})
	result := hasUnstableContainerStates(pod)

	assert.True(t, result)
}

func makePodWithContainerStatuses(containerStates []v1.ContainerState, initContainerStates []v1.ContainerState) *v1.Pod {
	containers := make([]v1.ContainerStatus, len(containerStates))
	for i, state := range containerStates {
		containers[i] = v1.ContainerStatus{
			State: state,
		}
	}

	initContainers := make([]v1.ContainerStatus, len(initContainerStates))
	for i, state := range initContainerStates {
		initContainers[i] = v1.ContainerStatus{
			State: state,
		}
	}
	pod := v1.Pod{
		Status: v1.PodStatus{
			ContainerStatuses:     containers,
			InitContainerStatuses: initContainers,
		},
	}

	return &pod
}

func TestExtractPodFailedReason(t *testing.T) {
	failedReason := ExtractPodFailedReason(evictedPod)
	assert.Equal(t, failedReason, evictedPod.Status.Message)

	failedReason = ExtractPodFailedReason(deadlineExceededPod)
	assert.Equal(t, failedReason, deadlineExceededPod.Status.Message)

	failedReason = ExtractPodFailedReason(oomPod)
	assert.True(t, strings.Contains(failedReason, oomPod.Status.ContainerStatuses[0].State.Terminated.Reason))

	failedReason = ExtractPodFailedReason(customErrorPod)
	assert.True(t, strings.Contains(failedReason, customErrorPod.Status.ContainerStatuses[0].State.Terminated.Message))
}

func TestExtractPodFailedCause(t *testing.T) {
	failedCause := ExtractPodFailedCause(evictedPod)
	assert.Equal(t, failedCause, api.Cause_Evicted)

	failedCause = ExtractPodFailedCause(deadlineExceededPod)
	assert.Equal(t, failedCause, api.Cause_DeadlineExceeded)

	failedCause = ExtractPodFailedCause(oomPod)
	assert.Equal(t, failedCause, api.Cause_OOM)

	failedCause = ExtractPodFailedCause(customErrorPod)
	assert.Equal(t, failedCause, api.Cause_Error)
}

func TestExtractFailedPodContainerStatuses(t *testing.T) {
	containerStatuses := ExtractFailedPodContainerStatuses(evictedPod)
	assert.Equal(t, len(containerStatuses), 0)

	containerStatuses = ExtractFailedPodContainerStatuses(deadlineExceededPod)
	assert.Equal(t, len(containerStatuses), 0)

	containerStatuses = ExtractFailedPodContainerStatuses(oomPod)
	assert.Equal(t, len(containerStatuses), 1)
	assert.Equal(t, containerStatuses[0].Reason, oomPod.Status.ContainerStatuses[0].State.Terminated.Reason)
	assert.Equal(t, containerStatuses[0].Cause, api.Cause_OOM)

	containerStatuses = ExtractFailedPodContainerStatuses(customErrorPod)
	assert.Equal(t, len(containerStatuses), 1)
	assert.Equal(t, containerStatuses[0].Message, customErrorPod.Status.ContainerStatuses[0].State.Terminated.Message)
	assert.Equal(t, containerStatuses[0].Cause, api.Cause_Error)
}

func createOomContainerStatus() v1.ContainerStatus {
	return v1.ContainerStatus{
		Name: "custom-error",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				ExitCode: 137,
				Reason:   "OOMKilled",
			},
		},
	}
}

func createCustomErrorContainerStatus() v1.ContainerStatus {
	return v1.ContainerStatus{
		Name: "custom-error",
		State: v1.ContainerState{
			Terminated: &v1.ContainerStateTerminated{
				ExitCode: 1,
				Reason:   "Error",
				Message:  "Custom error",
			},
		},
	}
}

func createFailedPod(containerStatuses ...v1.ContainerStatus) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase:             v1.PodFailed,
			ContainerStatuses: containerStatuses,
		},
	}
}

func createEvictedPod() *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase:   v1.PodFailed,
			Reason:  "Evicted",
			Message: "Pod ephemeral local storage usage exceeds the total limit of containers 1Gi.",
		},
	}
}

func createDeadlineExceededPod() *v1.Pod {
	// For DeadlineExceeded, Kubernetes leaves the container statuses as Running even though it kills them on the host
	containerStatus := v1.ContainerStatus{
		Name: "app",
		State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{
				StartedAt: metav1.NewTime(time.Now()),
			},
		},
	}
	return &v1.Pod{
		Status: v1.PodStatus{
			Phase:             v1.PodFailed,
			Reason:            "DeadlineExceeded",
			Message:           "Pod was active on the node longer than the specified deadline",
			ContainerStatuses: []v1.ContainerStatus{containerStatus},
		},
	}
}
