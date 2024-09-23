package schedulermocks

import (
	"golang.org/x/exp/slices"
)

type SliceMatcher struct {
	Expected []string
}

// Matches input against provided expected input
// This matching ignores the input ordering, so args don't need to be passed in a known order
func (s SliceMatcher) Matches(x interface{}) bool {
	inputs, ok := x.([]string)
	if !ok {
		return false
	}
	expected := s.Expected
	if len(inputs) != len(expected) {
		return false
	}
	slices.Sort(inputs)
	slices.Sort(expected)
	for i, inputValue := range inputs {
		if inputValue != expected[i] {
			return false
		}
	}
	return true
}

// String describes what the matcher matches.
func (s SliceMatcher) String() string {
	return "checks provided matches expected string list ignoring order"
}
