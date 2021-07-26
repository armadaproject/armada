package reporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/executor/domain"
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

func TestRequiresIngressToBeReported_FalseWhenIngressHasBeenReported(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				domain.HasIngress:      "true",
				domain.IngressReported: time.Now().String(),
			},
		},
	}
	assert.False(t, requiresIngressToBeReported(pod))
}

func TestRequiresIngressToBeReported_FalseWhenNonIngressPod(t *testing.T) {
	pod := &v1.Pod{}
	assert.False(t, requiresIngressToBeReported(pod))
}

func TestRequiresIngressToBeReported_TrueWhenHasIngressButNotIngressReportedAnnotation(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{domain.HasIngress: "true"},
		},
	}
	assert.True(t, requiresIngressToBeReported(pod))
}
