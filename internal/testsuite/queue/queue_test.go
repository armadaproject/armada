package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestQueueNameWithSuffix(t *testing.T) {
	spec := &api.TestSpec{Queue: "my-queue"}
	applyRandomSuffix(spec)
	assert.NotEqual(t, "my-queue", spec.Queue)
	assert.Contains(t, spec.Queue, "my-queue-")
	assert.Greater(t, len(spec.Queue), len("my-queue-"))
}
