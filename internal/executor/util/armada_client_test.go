package util

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestCreateEventMessageForCurrentState_WhenPodPending(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result, err := CreateEventMessageForCurrentState(&pod)
	assert.Nil(t, err)
	assert.NotNil(t, result.GetPending())

}

func TestCreateEventMessageForCurrentState_WhenPodRunning(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	result, err := CreateEventMessageForCurrentState(&pod)
	assert.Nil(t, err)
	assert.NotNil(t, result.GetRunning())
}

func TestCreateEventMessageForCurrentState_WhenPodFailed(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	result, err := CreateEventMessageForCurrentState(&pod)
	assert.Nil(t, err)
	assert.NotNil(t, result.GetFailed())
}

func TestCreateEventMessageForCurrentState_WhenPodSucceeded(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result, err := CreateEventMessageForCurrentState(&pod)
	assert.Nil(t, err)
	assert.NotNil(t, result.GetSucceeded())
}

func TestCreateEventMessageForCurrentState_ShouldError_WhenPodPhaseUnknown(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodUnknown,
		},
	}

	_, err := CreateEventMessageForCurrentState(&pod)
	assert.NotNil(t, err)
}
