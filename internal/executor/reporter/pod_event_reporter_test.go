package reporter

import (
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestIsInTerminalState_ShouldReturnTrueWhenPodInSucceededPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus {
			Phase: v1.PodSucceeded,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)

	if !inTerminatedState {
		t.Errorf("InTerminatedState was incorrect, got: %t want: %t", inTerminatedState, true)
	}
}

func TestIsInTerminalState_ShouldReturnTrueWhenPodInFailedPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus {
			Phase: v1.PodFailed,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)

	if !inTerminatedState {
		t.Errorf("InTerminatedState was incorrect, got: %t want: %t", inTerminatedState, true)
	}
}

func TestIsInTerminalState_ShouldReturnFalseWhenPodInNonTerminalState(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus {
			Phase: v1.PodPending,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)

	if inTerminatedState {
		t.Errorf("InTerminatedState was incorrect, got: %t want: %t", inTerminatedState, false)
	}
}
