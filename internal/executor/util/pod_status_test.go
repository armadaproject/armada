package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestIsRetryable_ReturnTrue_WhenNoContainerInImagePullBackoff(t *testing.T) {
	runningContainer := v1.ContainerState{
		Running: &v1.ContainerStateRunning{},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{runningContainer}, []v1.ContainerState{})
	result := IsRetryable(pod)

	assert.True(t, result)
}

func TestIsRetryable_ReturnFalse_WhenContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := IsRetryable(pod)

	assert.False(t, result)
}

func TestIsRetryable_ReturnFalse_WhenContainerInErrImagePull(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ErrImagePull",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := IsRetryable(pod)

	assert.False(t, result)
}

func TestIsRetryable_ReturnFalse_WhenInitContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{}, []v1.ContainerState{imagePullBackoffState})
	result := IsRetryable(pod)

	assert.False(t, result)
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
