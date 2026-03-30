package api

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestUnmarshalRetryAction_AcceptsCanonicalAliasAndNumeric(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected RetryAction
	}{
		"canonical FAIL":  {`"RETRY_ACTION_FAIL"`, RetryAction_RETRY_ACTION_FAIL},
		"canonical RETRY": {`"RETRY_ACTION_RETRY"`, RetryAction_RETRY_ACTION_RETRY},
		"alias Fail":      {`"Fail"`, RetryAction_RETRY_ACTION_FAIL},
		"alias Retry":     {`"Retry"`, RetryAction_RETRY_ACTION_RETRY},
		"alias FAIL":      {`"FAIL"`, RetryAction_RETRY_ACTION_FAIL},
		"alias retry":     {`"retry"`, RetryAction_RETRY_ACTION_RETRY},
		"numeric 1":       {`1`, RetryAction_RETRY_ACTION_FAIL},
		"numeric 2":       {`2`, RetryAction_RETRY_ACTION_RETRY},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var got RetryAction
			require.NoError(t, got.UnmarshalJSON([]byte(tc.input)))
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestUnmarshalRetryAction_RejectsInvalid(t *testing.T) {
	tests := map[string]string{
		"unknown name":        `"BANANAS"`,
		"out-of-range number": `99`,
		"non-string non-int":  `{}`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			var got RetryAction
			assert.Error(t, got.UnmarshalJSON([]byte(input)))
		})
	}
}

func TestUnmarshalExitCodeOperator_AcceptsCanonicalAliasAndNumeric(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected ExitCodeOperator
	}{
		"canonical IN":     {`"EXIT_CODE_OPERATOR_IN"`, ExitCodeOperator_EXIT_CODE_OPERATOR_IN},
		"canonical NOT_IN": {`"EXIT_CODE_OPERATOR_NOT_IN"`, ExitCodeOperator_EXIT_CODE_OPERATOR_NOT_IN},
		"alias In":         {`"In"`, ExitCodeOperator_EXIT_CODE_OPERATOR_IN},
		"alias NotIn":      {`"NotIn"`, ExitCodeOperator_EXIT_CODE_OPERATOR_NOT_IN},
		"alias notin":      {`"notin"`, ExitCodeOperator_EXIT_CODE_OPERATOR_NOT_IN},
		"numeric 1":        {`1`, ExitCodeOperator_EXIT_CODE_OPERATOR_IN},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var got ExitCodeOperator
			require.NoError(t, got.UnmarshalJSON([]byte(tc.input)))
			assert.Equal(t, tc.expected, got)
		})
	}
}
