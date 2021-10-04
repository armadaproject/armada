package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeMaps_AllValuesPresent(t *testing.T) {
	expected := map[string]string{
		"a": "value1",
		"b": "value2",
	}
	map1 := map[string]string{
		"a": "value1",
	}
	map2 := map[string]string{
		"b": "value2",
	}
	assert.Equal(t, MergeMaps(map1, map2), expected)
}

func TestMergeMaps_SecondMapOverwriteValues(t *testing.T) {
	expected := map[string]string{
		"a": "value2",
	}
	map1 := map[string]string{
		"a": "value1",
	}
	map2 := map[string]string{
		"a": "value2",
	}
	assert.Equal(t, MergeMaps(map1, map2), expected)
}

func TestMergeMaps_EmptyMap(t *testing.T) {
	expected := map[string]string{
		"a": "value1",
	}
	map1 := map[string]string{
		"a": "value1",
	}
	emptyMap := map[string]string{}
	assert.Equal(t, MergeMaps(map1, emptyMap), expected)
	assert.Equal(t, MergeMaps(emptyMap, emptyMap), emptyMap)
}

func TestMergeMaps_Nil(t *testing.T) {
	map1 := map[string]string{
		"a": "value1",
	}
	assert.Equal(t, MergeMaps(map1, nil), map1)
	assert.Equal(t, MergeMaps(nil, map1), map1)
	assert.Equal(t, MergeMaps(nil, nil), map[string]string{})
}

func TestEqual(t *testing.T) {
	map1 := map[string]string{
		"a": "value1",
	}
	map2 := DeepCopy(map1)
	assert.True(t, Equal(map1, map2))

	map3 := DeepCopy(map1)
	map3["a"] = "value2"
	assert.False(t, Equal(map1, map3))

	map4 := DeepCopy(map1)
	delete(map4, "a")
	assert.False(t, Equal(map1, map4))

	map5 := DeepCopy(map1)
	map5["b"] = "value2"
	assert.False(t, Equal(map1, map5))
}

func TestEqual_Nil(t *testing.T) {
	map1 := map[string]string{
		"a": "value1",
	}
	assert.False(t, Equal(map1, nil))
	assert.False(t, Equal(nil, map1))
	assert.True(t, Equal(nil, nil))
}

func Test_FilterKeys(t *testing.T) {
	assert.Equal(t, map[string]string{}, FilterKeys(map[string]string{}, []string{}))
	assert.Equal(t, map[string]string{}, FilterKeys(map[string]string{"a": "b"}, []string{}))
	assert.Equal(t, map[string]string{"a": "b"}, FilterKeys(map[string]string{"a": "b"}, []string{"a"}))
	assert.Equal(t, map[string]string{"a": "b"}, FilterKeys(map[string]string{"a": "b"}, []string{"a", "another"}))
	assert.Equal(t, map[string]string{"a": "b"}, FilterKeys(map[string]string{"a": "b", "c": "d"}, []string{"a", "another"}))
	assert.Equal(t, map[string]string{"a": "b", "c": "d"}, FilterKeys(map[string]string{"a": "b", "c": "d"}, []string{"a", "c", "another"}))
	assert.Equal(t, map[string]string{"a": "b"}, FilterKeys(map[string]string{"a": "b"}, []string{"a", "another", "a"}))
}

func Test_FilterKeys_Nil(t *testing.T) {
	assert.Nil(t, FilterKeys(nil, nil))
	assert.Nil(t, FilterKeys(nil, []string{}))
	assert.Equal(t, map[string]string{}, FilterKeys(map[string]string{"a": "b"}, nil))
}
