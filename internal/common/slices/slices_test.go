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

func TestPartitionFlatten(t *testing.T) {
	tests := map[string]struct {
		input    []int
		n        int
		expected [][]int
	}{
		"3 elements 1 partition": {
			input:    []int{1, 2, 3},
			n:        1,
			expected: [][]int{{1, 2, 3}},
		},
		"3 elements 2 partitions": {
			input:    []int{1, 2, 3},
			n:        2,
			expected: [][]int{{1, 2}, {3}},
		},
		"3 elements 3 partitions": {
			input:    []int{1, 2, 3},
			n:        3,
			expected: [][]int{{1}, {2}, {3}},
		},
		"3 elements 4 partitions": {
			input:    []int{1, 2, 3},
			n:        4,
			expected: [][]int{{1}, {2}, {3}, {}},
		},
		"empty input 3 partitions": {
			input:    []int{},
			n:        3,
			expected: [][]int{{}, {}, {}},
		},
		"nil input 3 partitions": {
			input:    nil,
			n:        3,
			expected: [][]int{nil, nil, nil},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			partitions := Partition(tc.input, tc.n)
			assert.Equal(t, tc.expected, partitions)
			flattened := Flatten(partitions)
			assert.Equal(t, tc.input, flattened)
		})
	}
}

func TestPartitionToMaxLen(t *testing.T) {
	tests := map[string]struct {
		input  []int
		maxLen int
	}{
		"3 elements maxLen 1": {
			input:  []int{1, 2, 3},
			maxLen: 1,
		},
		"3 elements maxLen 2": {
			input:  []int{1, 2, 3},
			maxLen: 2,
		},
		"3 elements maxLen 3": {
			input:  []int{1, 2, 3},
			maxLen: 3,
		},
		"3 elements maxLen 4": {
			input:  []int{1, 2, 3},
			maxLen: 4,
		},
		"empty input maxLen 3": {
			input:  []int{},
			maxLen: 3,
		},
		"nil input maxLen 3": {
			input:  nil,
			maxLen: 3,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			partitions := PartitionToMaxLen(tc.input, tc.maxLen)
			for _, partition := range partitions {
				assert.LessOrEqual(t, len(partition), tc.maxLen)
			}
			flattened := Flatten(partitions)
			assert.Equal(t, tc.input, flattened)
		})
	}
}

func TestGroupByFunc(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	actual := GroupByFunc(s, func(e int) int { return e / 3 })
	expected := map[int][]int{
		0: {1, 2},
		1: {3, 4, 5},
	}
	assert.Equal(t, expected, actual)
}

func TestMapAndGroupByFuncs(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	actual := MapAndGroupByFuncs(s, func(e int) int { return e / 3 }, func(e int) float64 { return float64(e) })
	expected := map[int][]float64{
		0: {1.0, 2.0},
		1: {3.0, 4.0, 5.0},
	}
	assert.Equal(t, expected, actual)
}

//func TestSubtract(t *testing.T) {
//
//	tests := map[string]struct {
//		input          []int
//		toSubtract     []int
//		expectedOutput []int
//	}{
//		"RemovesExpectedElements": {
//			input:          []int{1, 3, 5},
//			toSubtract:     []int{1, 5},
//			expectedOutput: []int{3},
//		},
//		"ToRemoveAndInputAreEqual": {
//			input:          []int{1, 3, 5, 7, 9},
//			toSubtract:     []int{1, 3, 5, 7, 9},
//			expectedOutput: []int{},
//		},
//		"ToRemoveIsLargerThanInput": {
//			input:          []int{1, 3, 5, 7, 9},
//			toSubtract:     []int{1, 3, 5, 7, 9},
//			expectedOutput: []int{},
//		},
//		"InputIsNil": {},
//		"ToRemoveIsNil": {},
//		"InputsAreNil": {},
//		"InputsAreEmpty": {},
//	}
//	for name, tc := range tests {
//		t.Run(name, func(t *testing.T) {
//			output := Subtract(tc.input, tc.toSubtract)
//			assert.Equal(t, output, tc.expectedOutput)
//		})
//	}
//}
//
//func TestSubtract_Empty(t *testing.T) {
//	input := []int{}
//	toSubtract := []int{}
//	expectedOutput := []int{}
//
//	output := Subtract(input, toSubtract)
//	assert.Equal(t, output, expectedOutput)
//}
