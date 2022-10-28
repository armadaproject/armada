package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	assert.Equal(t, 2, Min(2, 3))
	assert.Equal(t, 2, Min(3, 2))
	assert.Equal(t, 2, Min(2, 2))
}
