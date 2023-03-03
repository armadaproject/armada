package maps

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

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

func TestDeepCopy(t *testing.T) {
	m := map[string]mySlice{
		"foo": {1, 2, 3},
		"bar": {10, 20, 30},
	}
	actual := DeepCopy(m)
	m["foo"][0] = 100
	m["bar"] = append(m["bar"], 40)
	expected := map[string]mySlice{
		"foo": {1, 2, 3},
		"bar": {10, 20, 30},
	}
	assert.Equal(t, expected, actual)
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
