package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeserializeJobState(t *testing.T) {
	assert.NoError(t, testDeserializeJobStateString(t, "QUEUED"))
	assert.NoError(t, testDeserializeJobStateString(t, "PENDING"))
	assert.NoError(t, testDeserializeJobStateString(t, "RUNNING"))

	assert.NoError(t, testDeserializeJobStateInt(t, 0))
	assert.NoError(t, testDeserializeJobStateInt(t, 1))
	assert.NoError(t, testDeserializeJobStateInt(t, 2))
}

func TestDeserializeJobState_WhenInputInvalid(t *testing.T) {
	assert.Error(t, testDeserializeJobStateString(t, "INVALID"))
	assert.Error(t, testDeserializeJobStateInt(t, 3))
}

func testDeserializeJobStateString(t *testing.T, input string) error {
	value := input
	marshalled, err := json.Marshal(value)
	assert.NoError(t, err)
	return deserializeJobState(marshalled)
}

func testDeserializeJobStateInt(t *testing.T, input int) error {
	value := input
	marshalled, err := json.Marshal(value)
	assert.NoError(t, err)
	return deserializeJobState(marshalled)
}

func deserializeJobState(input []byte) error {
	type deserializerStruct struct {
		x JobState
	}
	deserializer := deserializerStruct{x: JobState_QUEUED}

	return deserializer.x.UnmarshalJSON(input)
}
