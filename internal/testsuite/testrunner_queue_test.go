package testsuite

import (
	"testing"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
)

func TestQueueNameWithSuffix(t *testing.T) {
	spec := &api.TestSpec{
		Queue:       "my-queue",
		QueueConfig: &api.QueueConfig{Setup: &api.QueueSetup{RandomSuffix: true}},
	}
	applyQueueRandomSuffix(spec)
	assert.NotEqual(t, "my-queue", spec.Queue)
	assert.Contains(t, spec.Queue, "my-queue-")
	assert.Greater(t, len(spec.Queue), len("my-queue-"))
}

func TestQueueNameNoSuffix(t *testing.T) {
	spec := &api.TestSpec{
		Queue:       "my-queue",
		QueueConfig: &api.QueueConfig{Setup: &api.QueueSetup{RandomSuffix: false}},
	}
	applyQueueRandomSuffix(spec)
	assert.Equal(t, "my-queue", spec.Queue)
}

func TestQueueNameNilSetup(t *testing.T) {
	spec := &api.TestSpec{Queue: "my-queue"}
	applyQueueRandomSuffix(spec)
	assert.Equal(t, "my-queue", spec.Queue)
}

func TestQueueNameForIndexSingle(t *testing.T) {
	assert.Equal(t, "my-queue", queueNameForIndex("my-queue", 0, 1, 1))
}

func TestQueueNameForIndexMultiple(t *testing.T) {
	assert.Equal(t, "my-queue-0", queueNameForIndex("my-queue", 0, 2, 3))
	assert.Equal(t, "my-queue-5", queueNameForIndex("my-queue", 5, 2, 3))
}

func TestQueueNameForIndexInfiniteBatches(t *testing.T) {
	assert.Equal(t, "my-queue-0", queueNameForIndex("my-queue", 0, 0, 1))
}
