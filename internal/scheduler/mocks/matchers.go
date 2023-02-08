package schedulermocks

import (
	"sort"
	"strings"

	"github.com/google/uuid"
)

type UuidSliceMatcher struct {
	Expected []uuid.UUID
}

func (s UuidSliceMatcher) Matches(x interface{}) bool {
	inputs, ok := x.([]uuid.UUID)
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
func (s UuidSliceMatcher) String() string {
	return "checks provided matches expected uuid list"
}
