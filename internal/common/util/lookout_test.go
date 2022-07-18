package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPreprocessErrorMessage(t *testing.T) {
	assert.Equal(t, "", TruncateAndRemoveNullsFromString("", 3))
	assert.Equal(t, "abc", TruncateAndRemoveNullsFromString("abc", 3))
	assert.Equal(t, "abc", TruncateAndRemoveNullsFromString("abcd", 3))
	assert.Equal(t, "abcd", TruncateAndRemoveNullsFromString("abcd\000e", 4))
	assert.Equal(t, "abc", TruncateAndRemoveNullsFromString("a\000b\000c\000d", 3))
}
