package reporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHasPodBeenInStateForLongerThanGivenDuration_ReturnsTrue(t *testing.T) {
	now := time.Now()
	sixSecondsAgo := now.Add(-6 * time.Second)

	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(sixSecondsAgo)}},
		},
	}

	result := HasPodBeenInStateForLongerThanGivenDuration(&pod, 5*time.Second)

	assert.True(t, result)

}

func TestHasPodBeenInStateForLongerThanGivenDuration_ReturnsFalse(t *testing.T) {
	now := time.Now()
	threeSecondsAgo := now.Add(-3 * time.Second)

	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(threeSecondsAgo)}},
		},
	}

	result := HasPodBeenInStateForLongerThanGivenDuration(&pod, 5*time.Second)

	assert.False(t, result)
}

func TestHasPodBeenInStateForLongerThanGivenDuration_ReturnsFalse_WhenNoPodStateChangesCanBeFound(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{},
		},
	}

	result := HasPodBeenInStateForLongerThanGivenDuration(&pod, 5*time.Second)

	assert.False(t, result)
}

func TestHasCurrentStateBeenReported_TrueWhenAnnotationExistsForCurrentPhase(t *testing.T) {
	podPhase := v1.PodRunning
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{string(podPhase): time.Now().String()},
		},
		Status: v1.PodStatus{
			Phase: podPhase,
		},
	}
	result := HasCurrentStateBeenReported(&pod)
	assert.True(t, result)
}

func TestHasCurrentStateBeenReported_FalseWhenNoAnnotationExistsForCurrentPhase(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			//Annotation for different phase
			Annotations: map[string]string{string(v1.PodPending): time.Now().String()},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	result := HasCurrentStateBeenReported(&pod)
	assert.False(t, result)
}

func TestHasCurrentStateBeenReported_FalseWhenNoAnnotationsExist(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	result := HasCurrentStateBeenReported(&pod)
	assert.False(t, result)
}
