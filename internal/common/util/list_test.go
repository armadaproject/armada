package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	includeOver5 := func(val int) bool { return val > 5 }
	input := []int{1, 3, 5, 7, 9}
	expectedOutput := []int{7, 9}

	output := Filter(input, includeOver5)
	assert.Equal(t, expectedOutput, output)
}

func TestFilter_ExcludeAllFilter(t *testing.T) {
	excludeAll := func(val int) bool { return false }
	input := []int{1, 3, 5, 7, 9}
	expectedOutput := []int(nil)

	output := Filter(input, excludeAll)
	assert.Equal(t, expectedOutput, output)
}

func TestFilter_IncludeAllFilter(t *testing.T) {
	includeAll := func(val int) bool { return true }
	input := []int{1, 3, 5, 7, 9}
	expectedOutput := input

	output := Filter(input, includeAll)
	assert.Equal(t, expectedOutput, output)
}

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
