package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	assert.Equal(t, 1, Max(0, 1))
	assert.Equal(t, 1, Max(1, 0))
	assert.Equal(t, 1.0, Max(0.0, 1.0))
	assert.Equal(t, 1.0, Max(1.0, 0.0))
	assert.Equal(t, 0, Max(0, -1))
	assert.Equal(t, 0, Max(-1, 0))
}
