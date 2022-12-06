package slices

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	toString := func(val int) string { return fmt.Sprintf("%d", val) }
	input := []int{1, 3, 5, 7, 9}
	expectedOutput := []string{"1", "3", "5", "7", "9"}

	output := Map(input, toString)
	assert.Equal(t, expectedOutput, output)
}

func TestMapEmptyList(t *testing.T) {
	toString := func(val int) string { return fmt.Sprintf("%d", val) }
	input := []int{}
	expectedOutput := []string{}

	output := Map(input, toString)
	assert.Equal(t, expectedOutput, output)
}
