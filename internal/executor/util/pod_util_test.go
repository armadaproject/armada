package util

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestIsInTerminalState_ShouldReturnTrueWhenPodInSucceededPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)
	assert.True(t, inTerminatedState)
}

func TestIsInTerminalState_ShouldReturnTrueWhenPodInFailedPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)
	assert.True(t, inTerminatedState)
}

func TestIsInTerminalState_ShouldReturnFalseWhenPodInNonTerminalState(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)

	assert.False(t, inTerminatedState)
}
