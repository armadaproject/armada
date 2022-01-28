package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalJsonIngressType(t *testing.T) {
	tests := map[string]IngressType{
		"0":           IngressType_Ingress,
		"\"Ingress\"": IngressType_Ingress,
	}

	for input, expected := range tests {
		var ingressType IngressType
		err := ingressType.UnmarshalJSON([]byte(input))
		assert.NoError(t, err)
		assert.Equal(t, expected, ingressType)
	}
}

func TestMarshalJsonIngressType_InvalidEnumValues(t *testing.T) {
	testInputs := []string{
		"100",
		"\"Invalid\"",
	}

	for _, input := range testInputs {
		var ingressType IngressType
		err := ingressType.UnmarshalJSON([]byte(input))
		assert.Error(t, err)
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
		var serviceType ServiceType
		err := serviceType.UnmarshalJSON([]byte(input))
		assert.NoError(t, err)
		assert.Equal(t, expected, serviceType)
	}
}

func TestMarshalJsonServiceType_InvalidEnumValues(t *testing.T) {
	testInputs := []string{
		"100",
		"\"Invalid\"",
	}

	for _, input := range testInputs {
		var serviceType ServiceType
		err := serviceType.UnmarshalJSON([]byte(input))
		assert.Error(t, err)
	}
}
