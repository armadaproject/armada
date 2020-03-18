package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestContainersAreRetryable_ReturnTrue_WhenNoContainerInImagePullBackoff(t *testing.T) {
	runningContainer := v1.ContainerState{
		Running: &v1.ContainerStateRunning{},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{runningContainer}, []v1.ContainerState{})
	result := ContainersAreRetryable(pod)

	assert.True(t, result)
}

func TestContainersAreRetryable_ReturnFalse_WhenContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := ContainersAreRetryable(pod)

	assert.False(t, result)
}

func TestContainersAreRetryable_ReturnFalse_WhenContainerInErrImagePull(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ErrImagePull",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{imagePullBackoffState}, []v1.ContainerState{})
	result := ContainersAreRetryable(pod)

	assert.False(t, result)
}

func TestContainersAreRetryable_ReturnFalse_WhenInitContainerInImagePullBackoff(t *testing.T) {
	imagePullBackoffState := v1.ContainerState{
		Waiting: &v1.ContainerStateWaiting{
			Reason: "ImagePullBackOff",
		},
	}

	pod := makePodWithContainerStatuses([]v1.ContainerState{}, []v1.ContainerState{imagePullBackoffState})
	result := ContainersAreRetryable(pod)

	assert.False(t, result)
}

func TestDiagnoseStuckPod_ShouldRetryWithNoProblems(t *testing.T) {
	waitingContainer := v1.ContainerState{Waiting: &v1.ContainerStateWaiting{}}
	pod := makePodWithContainerStatuses([]v1.ContainerState{waitingContainer}, []v1.ContainerState{})
	events := []*v1.Event{}

	retryable, _ := DiagnoseStuckPod(pod, events)
	assert.True(t, retryable)
}

func TestDiagnoseStuckPod_ShouldReportUnexpectedWarnings(t *testing.T) {
	waitingContainer := v1.ContainerState{Waiting: &v1.ContainerStateWaiting{}}
	pod := makePodWithContainerStatuses([]v1.ContainerState{waitingContainer}, []v1.ContainerState{})
	events := []*v1.Event{&v1.Event{Reason: "PodExploded", Type: v1.EventTypeWarning, Message: "Boom"}}

	retryable, message := DiagnoseStuckPod(pod, events)
	assert.False(t, retryable)
	assert.Contains(t, message, "Boom")
}

func TestDiagnoseStuckPod_ShouldIgnoreSchedulingFailures(t *testing.T) {
	waitingContainer := v1.ContainerState{Waiting: &v1.ContainerStateWaiting{}}
	pod := makePodWithContainerStatuses([]v1.ContainerState{waitingContainer}, []v1.ContainerState{})
	events := []*v1.Event{&v1.Event{Reason: "FailedScheduling", Type: v1.EventTypeWarning}}

	retryable, _ := DiagnoseStuckPod(pod, events)
	assert.True(t, retryable)
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
