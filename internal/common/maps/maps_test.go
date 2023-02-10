package maps

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
