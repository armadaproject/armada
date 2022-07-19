package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncate(t *testing.T) {
	assert.Equal(t, "", Truncate("", 3))
	assert.Equal(t, "abc", Truncate("abc", 3))
	assert.Equal(t, "abc", Truncate("abcd", 3))
}
