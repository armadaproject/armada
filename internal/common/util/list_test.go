package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
