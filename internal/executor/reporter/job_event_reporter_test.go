package reporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLastStatusChange_ReturnsExpectedValue(t *testing.T) {
	earliest := time.Date(2019, 11, 20, 9, 30, 3, 0, time.UTC)
	middle := time.Date(2019, 11, 20, 9, 31, 4, 0, time.UTC)
	latest := time.Date(2019, 11, 20, 9, 31, 5, 0, time.UTC)
	conditions := []v1.PodCondition{
		{
			LastTransitionTime: metav1.NewTime(earliest),
		},
		{
			LastTransitionTime: metav1.NewTime(latest),
		},
		{
			LastTransitionTime: metav1.NewTime(middle),
		},
	}

	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: conditions,
		},
	}

	result, _ := lastStatusChange(&pod)

	assert.Equal(t, result, latest)
}

func TestLastStatusChange_ReturnsError_WhenNoStateChangesFound(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: make([]v1.PodCondition, 0),
		},
	}

	_, err := lastStatusChange(&pod)

	assert.NotNil(t, err)
}

func TestLastStatusChange_ReturnsCreatedTime_WhenNoStateChangesFoundForPendingPod(t *testing.T) {
	creationTime := time.Date(2019, 11, 20, 9, 31, 5, 0, time.UTC)
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(creationTime),
		},
		Status: v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: make([]v1.PodCondition, 0),
		},
	}
	result, _ := lastStatusChange(&pod)

	assert.Equal(t, result, creationTime)
}

func TestLastStatusChange_ReturnsError_WhenNoStateChangesFoundAndCreatedTimeIsNotSetForPendingPod(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: make([]v1.PodCondition, 0),
		},
	}
	_, err := lastStatusChange(&pod)

	assert.NotNil(t, err)
}

func TestLastStatusChange_ReportsTimeFromContainerStatus(t *testing.T) {
	now := time.Now()
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{{
				State: v1.ContainerState{Running: &v1.ContainerStateRunning{
					StartedAt: metav1.NewTime(now),
				}},
			}},
		},
	}
	result, err := lastStatusChange(&pod)
	assert.Equal(t, result, now)
	assert.Nil(t, err)

	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			FinishedAt: metav1.NewTime(now),
		}},
	}}
	result, err = lastStatusChange(&pod)
	assert.Equal(t, result, now)
	assert.Nil(t, err)
}

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
