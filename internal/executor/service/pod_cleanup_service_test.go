package service

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
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

func TestLastStatusChange_ReturnsErrorWhenNoStateChangesFound(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: make([]v1.PodCondition, 0),
		},
	}

	_, err := lastStatusChange(&pod)

	assert.True(t, err != nil)
}

func TestFilterPodsInStateForLongerThanGivenDuration(t *testing.T) {
	now := time.Now()
	threeSecondsAgo := now.Add(-3 * time.Second)
	sixSecondsAgo := now.Add(-6 * time.Second)

	pod1 := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(threeSecondsAgo)}},
		},
	}

	pod2 := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(sixSecondsAgo)}},
		},
	}

	result := filterPodsInStateForLongerThanGivenDuration([]*v1.Pod{&pod1, &pod2}, 5*time.Second)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &pod2)

}

func TestFilterPodsInStateForLongerThanGivenDuration_ReturnsEmptyIfNoPodsInStateForLongerThanGivenDuration(t *testing.T) {
	now := time.Now()
	threeSecondsAgo := now.Add(-3 * time.Second)
	fourSecondsAgo := now.Add(-4 * time.Second)

	pod1 := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(threeSecondsAgo)}},
		},
	}

	pod2 := v1.Pod{
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{{LastTransitionTime: metav1.NewTime(fourSecondsAgo)}},
		},
	}

	result := filterPodsInStateForLongerThanGivenDuration([]*v1.Pod{&pod1, &pod2}, 5*time.Second)

	assert.Equal(t, len(result), 0)
}

func TestFilterCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	completedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result := filterCompletedPods([]*v1.Pod{&runningPod, &completedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &completedPod)
}

func TestFilterCompletedPods_ShouldReturnEmptyIfNoCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	pendingPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result := filterCompletedPods([]*v1.Pod{&runningPod, &pendingPod})

	assert.Equal(t, len(result), 0)
}
