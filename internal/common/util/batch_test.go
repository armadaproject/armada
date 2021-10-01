package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	assert.Equal(t, [][]string{}, Batch([]string{}, 1))
	assert.Equal(t, [][]string{{"a"}}, Batch([]string{"a"}, 1))
	assert.Equal(t, [][]string{{"a"}}, Batch([]string{"a"}, 10))
	assert.Equal(t, [][]string{{"a"}, {"b"}}, Batch([]string{"a", "b"}, 1))
	assert.Equal(t, [][]string{{"a", "b"}}, Batch([]string{"a", "b"}, 2))
	assert.Equal(t, [][]string{{"a", "b"}}, Batch([]string{"a", "b"}, 3))
	assert.Equal(t, [][]string{{"a", "b", "c"}, {"d", "e"}}, Batch([]string{"a", "b", "c", "d", "e"}, 3))
	assert.Equal(t, [][]string{{"a", "b", "c"}, {"d", "e", "f"}}, Batch([]string{"a", "b", "c", "d", "e", "f"}, 3))
}
