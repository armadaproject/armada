package schedulermocks

import (
	"sort"
	"strings"
)

type Stringer interface {
	String() string
}

type SliceMatcher[T Stringer] struct {
	Expected []T
}

// Matches
// Matches input against provided expected input
// This matching ignores the input ordering, so args don't need to be passed in a known order
func (s SliceMatcher[T]) Matches(x interface{}) bool {
	inputs, ok := x.([]T)
	if !ok {
		return false
	}
	expected := s.Expected
	if len(inputs) != len(expected) {
		return false
	}
	sort.Slice(inputs, func(i, j int) bool {
		return strings.Compare(inputs[i].String(), inputs[j].String()) < 0
	})
	sort.Slice(expected, func(i, j int) bool {
		return strings.Compare(expected[i].String(), expected[j].String()) < 0
	})
	for i, inputValue := range inputs {
		if inputValue.String() != expected[i].String() {
			return false
		}
	}
	return true
}

// String describes what the matcher matches.
func (s SliceMatcher[T]) String() string {
	return "checks provided matches expected uuid list"
}
