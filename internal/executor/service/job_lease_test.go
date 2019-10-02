package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetFinishedPods(t *testing.T) {
	nonFinishedPod := makePodWithCurrentStateReported(v1.PodPending)
	finishedPod := makePodWithCurrentStateReported(v1.PodSucceeded)

	result := getFinishedPods([]*v1.Pod{nonFinishedPod, finishedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], finishedPod)
}

func TestGetFinishedPods_HandlesEmptyInput(t *testing.T) {
	result := getFinishedPods([]*v1.Pod{})

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestIsPodReadyForCleanup_ReturnsTrue_WhenPodCompletedWithStateReported(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodSucceeded)

	result := isPodReadyForCleanup(pod)
	assert.True(t, result)
}

func TestIsPodReadyForCleanup_ReturnsFalse_WhenPodNotCompleted(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodRunning)

	result := isPodReadyForCleanup(pod)
	assert.False(t, result)
}

func TestIsPodReadyForCleanup_ReturnsFalse_WhenPodsStateNotReported(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodSucceeded)
	//Remove annotation marking pod state reported
	pod.Annotations = map[string]string{}

	result := isPodReadyForCleanup(pod)
	assert.False(t, result)
}

func makePodWithCurrentStateReported(state v1.PodPhase) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{string(state): time.Now().String()},
		},
		Status: v1.PodStatus{
			Phase: state,
		},
	}
	return &pod
}
