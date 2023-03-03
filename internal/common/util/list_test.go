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
	expectedOutput := []int{}
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

func TestFilter_Empty(t *testing.T) {
	includeAll := func(val int) bool { return true }
	input := []int{}
	expectedOutput := input

	output := Filter(input, includeAll)
	assert.Equal(t, expectedOutput, output)
}

func TestFilter_Nil(t *testing.T) {
	includeAll := func(val int) bool { return true }
	var input []int = nil
	expectedOutput := input

	output := Filter(input, includeAll)
	assert.Equal(t, expectedOutput, output)
}

func TestConcat_Empty(t *testing.T) {
	output := Concat([]int{})
	assert.Equal(t, []int{}, output)
}

func TestConcat(t *testing.T) {
	output := Concat(
		[]int{1, 2, 3},
		[]int{4, 5, 6, 7},
		[]int{8, 9, 10, 11, 12},
	)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, output)
}

func TestMap_Empty(t *testing.T) {
	output := Map([]int{}, func(val int) int { return val })
	assert.Equal(t, []int{}, output)
}

func TestMap(t *testing.T) {
	output := Map([]int{2, 4, 6, 11}, func(val int) string { return fmt.Sprintf("%d", val) })
	assert.Equal(t, []string{"2", "4", "6", "11"}, output)
}
