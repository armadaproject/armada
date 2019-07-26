package service

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestIsPodReadyForCleanup_ReturnsTrue_WhenPodCompletedWithStateReported(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodSucceeded)

	result := IsPodReadyForCleanup(pod)
	assert.True(t, result)
}

func TestIsPodReadyForCleanup_ReturnsFalse_WhenPodNotCompleted(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodRunning)

	result := IsPodReadyForCleanup(pod)
	assert.False(t, result)
}

func TestIsPodReadyForCleanup_ReturnsFalse_WhenPodsStateNotReported(t *testing.T) {
	pod := makePodWithCurrentStateReported(v1.PodSucceeded)
	//Remove annotation marking pod state reported
	pod.Annotations = map[string]string{}

	result := IsPodReadyForCleanup(pod)
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
