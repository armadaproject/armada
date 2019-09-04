package service

import (
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestGetRunningPods(t *testing.T) {
	nonFinishedPod := makePodWithCurrentStateReported(v1.PodPending)
	finishedPod := makePodWithCurrentStateReported(v1.PodSucceeded)

	result := getRunningPods([]*v1.Pod{nonFinishedPod, finishedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], nonFinishedPod)
}

func TestGetRunningPods_HandlesEmptyInput(t *testing.T) {
	result := getRunningPods([]*v1.Pod{})

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

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
