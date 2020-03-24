package reporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/pkg/api"
)

func TestCreateEventForCurrentState_WhenPodPending(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobPendingEvent)
	assert.True(t, ok)

}

func TestCreateEventForCurrentState_WhenPodRunning(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobRunningEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodFailed(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobFailedEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_WhenPodSucceeded(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.Nil(t, err)

	_, ok := result.(*api.JobSucceededEvent)
	assert.True(t, ok)
}

func TestCreateEventForCurrentState_ShouldError_WhenPodPhaseUnknown(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodUnknown,
		},
	}

	_, err := CreateEventForCurrentState(&pod, "cluster1")
	assert.NotNil(t, err)
}
