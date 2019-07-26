package service

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestSplitRunningAndFinishedPods(t *testing.T) {
	nonFinishedPod := makePodWithCurrentStateReported(v1.PodPending)
	finishedPod := makePodWithCurrentStateReported(v1.PodSucceeded)

	runningPods, finishedPods := splitRunningAndFinishedPods([]*v1.Pod{nonFinishedPod, finishedPod})

	assert.Equal(t, len(runningPods), 1)
	assert.Equal(t, runningPods[0], nonFinishedPod)

	assert.Equal(t, len(finishedPods), 1)
	assert.Equal(t, finishedPods[0], finishedPod)
}

func TestSplitRunningAndFinishedPods_HandlesEmptyInput(t *testing.T) {
	runningPods, finishedPods := splitRunningAndFinishedPods([]*v1.Pod{})

	assert.NotNil(t, runningPods)
	assert.Equal(t, len(runningPods), 0)
	assert.NotNil(t, finishedPods)
	assert.Equal(t, len(finishedPods), 0)
}
