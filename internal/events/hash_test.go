package events

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashLength(t *testing.T) {
	assert.Equal(t, len(HashFromQueueJobSetName("a", "b")), 32)
}

func TestHashCollision(t *testing.T) {
	assert.NotEqual(t, HashFromQueueJobSetName("aa", "b"), HashFromQueueJobSetName("a", "ab"))
}
