package slices

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestSubtract(t *testing.T) {
	tests := map[string]struct {
		input          []int
		toSubtract     []int
		expectedOutput []int
	}{
		"RemovesExpectedElements": {
			input:          []int{1, 3, 5},
			toSubtract:     []int{1, 5},
			expectedOutput: []int{3},
		},
		"ToRemoveAndInputAreEqual": {
			input:          []int{1, 3, 5, 7, 9},
			toSubtract:     []int{1, 3, 5, 7, 9},
			expectedOutput: []int{},
		},
		"ToRemoveIsLargerThanInput": {
			input:          []int{1, 5, 9},
			toSubtract:     []int{1, 3, 5, 7, 9},
			expectedOutput: []int{},
		},
		"InputIsNil": {
			input:          nil,
			toSubtract:     []int{1, 3, 5, 7, 9},
			expectedOutput: nil,
		},
		"ToRemoveIsNil": {
			input:          []int{1, 3, 5, 7, 9},
			toSubtract:     nil,
			expectedOutput: []int{1, 3, 5, 7, 9},
		},
		"AllInputsAreNil": {
			input:          nil,
			toSubtract:     nil,
			expectedOutput: nil,
		},
		"AllInputsAreEmpty": {
			input:          []int{},
			toSubtract:     []int{},
			expectedOutput: []int{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			output := Subtract(tc.input, tc.toSubtract)
			assert.Equal(t, output, tc.expectedOutput)
		})
	}
}

func TestUnique(t *testing.T) {
	tests := map[string]struct {
		s        []int
		expected []int
	}{
		"nil": {
			s:        nil,
			expected: nil,
		},
		"empty": {
			s:        make([]int, 0),
			expected: make([]int, 0),
		},
		"no duplicates": {
			s:        []int{1, 2, 3},
			expected: []int{1, 2, 3},
		},
		"consecutive duplicates": {
			s:        []int{1, 2, 3, 3},
			expected: []int{1, 2, 3},
		},
		"non-consecutive duplicates": {
			s:        []int{3, 1, 2, 3},
			expected: []int{3, 1, 2},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Unique(tc.s))
		})
	}
}

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

func TestPop(t *testing.T) {
	s := []int{1, 2, 3}
	expected := []int{3, 2, 1}
	var actual []int
	for len(s) > 0 {
		actual = append(actual, Pop(&s))
	}
	assert.Equal(t, expected, actual)
	assert.Panics(
		t,
		func() {
			Pop(&s)
		},
	)
	assert.Panics(
		t,
		func() {
			var s []int
			Pop(&s)
		},
	)
}

func TestRepeat(t *testing.T) {
	tests := map[string]struct {
		n        int
		vs       []int
		expected []int
	}{
		"n: 0": {
			n:        0,
			vs:       []int{1, 2, 3},
			expected: []int{},
		},
		"vs emptyy": {
			n:        3,
			vs:       []int{},
			expected: []int{},
		},
		"one entry": {
			n:        3,
			vs:       []int{0},
			expected: []int{0, 0, 0},
		},
		"multiple entries": {
			n:        3,
			vs:       []int{0, 1},
			expected: []int{0, 1, 0, 1, 0, 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, Repeat(tc.n, tc.vs...))
		})
	}
}

func TestAny(t *testing.T) {
	var nilSlice []int
	assert.Equal(
		t,
		false,
		AnyFunc(nilSlice, func(v int) bool { return true }),
	)

	emptySlice := make([]int, 0)
	assert.Equal(
		t,
		false,
		AnyFunc(emptySlice, func(v int) bool { return true }),
	)

	assert.Equal(
		t,
		true,
		AnyFunc([]int{1, 2, 3}, func(v int) bool { return v > 2 }),
	)

	assert.Equal(
		t,
		false,
		AnyFunc([]int{1, 2, 3}, func(v int) bool { return v > 3 }),
	)
}

func TestZeros(t *testing.T) {
	assert.Equal(t, make([]int, 3), Zeros[int](3))
	assert.Equal(t, make([]string, 3), Zeros[string](3))
}

func TestOnes(t *testing.T) {
	assert.Equal(t, []int{1, 1, 1}, Ones[int](3))
	assert.Equal(t, []float64{1, 1, 1}, Ones[float64](3))
}

func TestFill(t *testing.T) {
	assert.Equal(t, []int{2, 2, 2}, Fill[int](2, 3))
	assert.Equal(t, []float64{0.5, 0.5, 0.5}, Fill[float64](0.5, 3))
}
