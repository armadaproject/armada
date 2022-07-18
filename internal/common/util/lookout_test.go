package util

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveNullsFromString(t *testing.T) {
	s := RemoveNullsFromString("Hello \000 World")
	assert.NotContains(t, s, "\000")
}

func TestRemoveNullsFromJson(t *testing.T) {
	someMap := map[string]string{
		"hello": "world",
		"one":   "\000 two",
	}
	jsonData, err := json.Marshal(someMap)
	assert.NoError(t, err)

	s := RemoveNullsFromJson(jsonData)
	assert.NotContains(t, string(s), "\\u000")
}
