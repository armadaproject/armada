package api

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalJsonIngressType(t *testing.T) {
	tests := map[string]IngressType{
		"0":           IngressType_Ingress,
		"\"Ingress\"": IngressType_Ingress,
	}

	for input, expected := range tests {
		t.Run(fmt.Sprintf("TestMarshalJsonIngressType(%s)", input),
			func(t *testing.T) {
				var ingressType IngressType
				err := ingressType.UnmarshalJSON([]byte(input))
				assert.NoError(t, err)
				assert.Equal(t, expected, ingressType)
			})
	}
}

func TestMarshalJsonIngressType_InvalidEnumValues(t *testing.T) {
	testInputs := []string{
		"100",
		"\"Invalid\"",
	}

	for _, input := range testInputs {
		t.Run(fmt.Sprintf("TestMarshalJsonIngressTypes_Invalid(%s)", input),
			func(t *testing.T) {
				var ingressType IngressType
				err := ingressType.UnmarshalJSON([]byte(input))
				assert.Error(t, err)
			})
	}
}

func TestMarshalJsonServiceType(t *testing.T) {
	tests := map[string]ServiceType{
		"0":            ServiceType_NodePort,
		"\"NodePort\"": ServiceType_NodePort,
		"1":            ServiceType_Headless,
		"\"Headless\"": ServiceType_Headless,
	}

	for input, expected := range tests {
		t.Run(fmt.Sprintf("TestMarshalJsonServiceType(%s)", input),
			func(t *testing.T) {
				var serviceType ServiceType
				err := serviceType.UnmarshalJSON([]byte(input))
				assert.NoError(t, err)
				assert.Equal(t, expected, serviceType)
			})
	}
}

func TestMarshalJsonServiceType_InvalidEnumValues(t *testing.T) {
	testInputs := []string{
		"100",
		"\"Invalid\"",
	}

	for _, input := range testInputs {
		t.Run(fmt.Sprintf("TestMarshalJsonService_Invalid(%s)", input),
			func(t *testing.T) {
				var serviceType ServiceType
				err := serviceType.UnmarshalJSON([]byte(input))
				assert.Error(t, err)
			})
	}
}

func TestDeserializeJobState(t *testing.T) {
	testInputsStrings := []string{
		"QUEUED",
		"PENDING",
		"RUNNING",
	}

	for _, input := range testInputsStrings {
		t.Run(fmt.Sprintf("TestDeserializeJobStateString(%s)", input),
			func(t *testing.T) {
				assert.NoError(t, testDeserializeJobStateString(t, input))
			})
	}
}

func TestDeserializeJobStateInt(t *testing.T) {
	testInputsStrings := []int{
		0,
		1,
		2,
	}

	for _, input := range testInputsStrings {
		t.Run(fmt.Sprintf("TestDeserializeJobStateString(%d)", input),
			func(t *testing.T) {
				assert.NoError(t, testDeserializeJobStateInt(t, input))
			})
	}
}

func TestDeserializeJobState_WhenInputInvalid(t *testing.T) {
	assert.Error(t, testDeserializeJobStateString(t, "INVALID"))
	assert.Error(t, testDeserializeJobStateInt(t, 99))
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
