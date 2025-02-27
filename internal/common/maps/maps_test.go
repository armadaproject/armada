package maps

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestFromSlice(t *testing.T) {
	actual := FromSlice(
		[]int{1, 2},
		func(elem int) string { return strconv.Itoa(elem) },
		func(elem int) float64 { return float64(elem) * 0.1 },
	)
	expected := map[string]float64{
		"1": 0.1,
		"2": 0.2,
	}
	assert.Equal(t, expected, actual)
}

func TestMapKeys(t *testing.T) {
	m := map[string][]int{
		"foo":    {1, 2, 3},
		"foobar": {10, 20, 30},
	}
	actual := MapKeys(
		m,
		func(k string) int { return len(k) },
	)
	expected := map[int][]int{
		3: {1, 2, 3},
		6: {10, 20, 30},
	}
	assert.Equal(t, expected, actual)
}

func TestMapValues(t *testing.T) {
	m := map[string][]int{
		"foo":    {1, 2, 3},
		"foobar": {10, 20, 30},
	}
	actual := MapValues(
		m,
		func(v []int) int {
			rv := 0
			for _, vi := range v {
				rv += vi
			}
			return rv
		},
	)
	expected := map[string]int{
		"foo":    6,
		"foobar": 60,
	}
	assert.Equal(t, expected, actual)
}

func TestMap(t *testing.T) {
	m := map[string][]int{
		"foo":    {1, 2, 3},
		"foobar": {10, 20, 30},
	}
	actual := Map(
		m,
		func(k string) int { return len(k) },
		func(v []int) int {
			rv := 0
			for _, vi := range v {
				rv += vi
			}
			return rv
		},
	)
	expected := map[int]int{
		3: 6,
		6: 60,
	}
	assert.Equal(t, expected, actual)
}

type mySlice []int

func (s mySlice) DeepCopy() mySlice {
	return slices.Clone(s)
}

func (a mySlice) Equal(b mySlice) bool {
	return slices.Equal(a, b)
}

func TestEqual(t *testing.T) {
	tests := map[string]struct {
		a        map[string]mySlice
		b        map[string]mySlice
		expected bool
	}{
		"equal": {
			a: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 30},
			},
			b: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 30},
			},
			expected: true,
		},
		"missing key in a": {
			a: map[string]mySlice{
				"foo": {1, 2, 3},
			},
			b: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 30},
			},
			expected: false,
		},
		"missing key in b": {
			a: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 30},
			},
			b: map[string]mySlice{
				"foo": {1, 2, 3},
			},
			expected: false,
		},
		"differing value": {
			a: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 30},
			},
			b: map[string]mySlice{
				"foo": {1, 2, 3},
				"bar": {10, 20, 40},
			},
			expected: false,
		},
		"both nil": {
			expected: true,
		},
		"both empty": {
			a:        make(map[string]mySlice),
			b:        make(map[string]mySlice),
			expected: true,
		},
		"nil and empty": {
			a:        make(map[string]mySlice),
			b:        nil,
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, DeepEqual(tc.a, tc.b))
		})
	}
}

func TestFilterKeys(t *testing.T) {
	m := map[string][]int{
		"foo":    {1, 2, 3},
		"foobar": {10, 20, 30},
	}
	actual := FilterKeys(
		m,
		func(k string) bool { return len(k) > 3 },
	)
	expected := map[string][]int{
		"foobar": {10, 20, 30},
	}
	assert.Equal(t, expected, actual)
}

func TestRemoveInPlace(t *testing.T) {
	m := map[int]string{
		1: "one",
		2: "two",
		3: "three",
		4: "four",
	}
	RemoveInPlace(m, func(i int) bool {
		return i > 2
	})
	expected := map[int]string{
		1: "one",
		2: "two",
	}
	assert.Equal(t, expected, m)
}

func TestKeys(t *testing.T) {
	m := map[int]string{
		1: "one",
		2: "two",
	}
	result := Keys(m)
	slices.Sort(result)
	expected := []int{1, 2}
	assert.Equal(t, expected, result)
}
