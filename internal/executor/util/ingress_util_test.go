package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

func TestDeepCopy(t *testing.T) {
	input := &IngressServiceConfig{
		Type:  NodePort,
		Ports: []uint32{1, 2, 3},
		Annotations: map[string]string{
			"a": "value",
			"b": "value2",
		},
	}
	result := deepCopy(input)
	assert.Equal(t, input, result)

	result.Annotations["c"] = "value3"
	assert.NotEqual(t, input, result)

	result = deepCopy(input)
	result.Ports = append(result.Ports, 4)
	assert.NotEqual(t, input, result)
}

func TestGetServicePorts(t *testing.T) {
	config := &IngressServiceConfig{
		Ports: []uint32{1, 2, 3},
	}
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "a",
				Ports: []v1.ContainerPort{
					{
						ContainerPort: 1,
						Protocol:      v1.ProtocolTCP,
					},
					{
						ContainerPort: 2,
						Protocol:      v1.ProtocolUDP,
					},
				},
			},
		},
	}
	expected := []v1.ServicePort{
		{
			Name:     "a-1",
			Protocol: v1.ProtocolTCP,
			Port:     1,
		},
		{
			Name:     "a-2",
			Protocol: v1.ProtocolUDP,
			Port:     2,
		},
	}

	assert.Equal(t, GetServicePorts([]*IngressServiceConfig{config}, podSpec), expected)
}

func TestGetServicePorts_MultipleContainer(t *testing.T) {
	config := &IngressServiceConfig{
		Ports: []uint32{1, 2, 3},
	}
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "a",
				Ports: []v1.ContainerPort{
					{
						ContainerPort: 1,
						Protocol:      v1.ProtocolTCP,
					},
				},
			},
			{
				Name: "b",
				Ports: []v1.ContainerPort{
					{
						ContainerPort: 2,
						Protocol:      v1.ProtocolUDP,
					},
				},
			},
		},
	}
	expected := []v1.ServicePort{
		{
			Name:     "a-1",
			Protocol: v1.ProtocolTCP,
			Port:     1,
		},
		{
			Name:     "b-2",
			Protocol: v1.ProtocolUDP,
			Port:     2,
		},
	}

	assert.Equal(t, GetServicePorts([]*IngressServiceConfig{config}, podSpec), expected)
}

func TestGetServicePorts_MultipleIngressConfigs(t *testing.T) {
	config1 := &IngressServiceConfig{
		Ports: []uint32{1},
	}
	config2 := &IngressServiceConfig{
		Ports: []uint32{2},
	}
	config3 := &IngressServiceConfig{
		Ports: []uint32{3},
	}
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "a",
				Ports: []v1.ContainerPort{
					{
						ContainerPort: 1,
						Protocol:      v1.ProtocolTCP,
					},
					{
						ContainerPort: 2,
						Protocol:      v1.ProtocolUDP,
					},
				},
			},
		},
	}
	expected := []v1.ServicePort{
		{
			Name:     "a-1",
			Protocol: v1.ProtocolTCP,
			Port:     1,
		},
		{
			Name:     "a-2",
			Protocol: v1.ProtocolUDP,
			Port:     2,
		},
	}
	servicePorts := GetServicePorts([]*IngressServiceConfig{config1, config2, config3}, podSpec)
	assert.Equal(t, servicePorts, expected)
}

func TestGetServicePorts_HostPortSkipped(t *testing.T) {
	config := &IngressServiceConfig{
		Ports: []uint32{1, 2, 3},
	}
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "a",
				Ports: []v1.ContainerPort{
					{
						ContainerPort: 1,
						HostPort:      100,
						Protocol:      v1.ProtocolTCP,
					},
					{
						ContainerPort: 2,
						Protocol:      v1.ProtocolUDP,
					},
				},
			},
		},
	}
	expected := []v1.ServicePort{
		{
			Name:     "a-2",
			Protocol: v1.ProtocolUDP,
			Port:     2,
		},
	}

	assert.Equal(t, GetServicePorts([]*IngressServiceConfig{config}, podSpec), expected)
}

func TestGroupIngressConfig_IngressTypeNodePort_AlwaysGrouped(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		NodePort: {
			{
				Type:  NodePort,
				Ports: []uint32{1, 2, 3},
			},
		},
	}
	input1 := &IngressServiceConfig{
		Type:  NodePort,
		Ports: []uint32{1, 2},
	}
	input2 := &IngressServiceConfig{
		Type:  NodePort,
		Ports: []uint32{3},
	}
	groupedConfig := groupIngressConfig([]*IngressServiceConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)

	// Non ingress type will never have annotations anymore
	assert.Equal(t, groupIngressConfig([]*IngressServiceConfig{input1, input2}), expected)
}

func TestGroupIngressConfig_IngressType_NoAnnotations(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		Ingress: {
			{
				Type:  Ingress,
				Ports: []uint32{1, 2, 3},
			},
		},
	}
	input1 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{1, 2},
	}
	input2 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{3},
	}
	groupedConfig := groupIngressConfig([]*IngressServiceConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_IngressType_SameAnnotations(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		Ingress: {
			{
				Type:  Ingress,
				Ports: []uint32{1, 2, 3},
				Annotations: map[string]string{
					"test": "value",
				},
			},
		},
	}
	input1 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	assert.Equal(t, groupIngressConfig([]*IngressServiceConfig{input1, input2}), expected)
}

func TestGroupIngressConfig_IngressType_DifferentAnnotations(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		Ingress: {
			{
				Type:  Ingress,
				Ports: []uint32{1, 2},
				Annotations: map[string]string{
					"test": "value",
				},
			},
			{
				Type:  Ingress,
				Ports: []uint32{3},
				Annotations: map[string]string{
					"test": "value2",
				},
			},
		},
	}
	input1 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value2",
		},
	}
	groupedConfig := groupIngressConfig([]*IngressServiceConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_MixedIngressType(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		Ingress: {
			{
				Type:  Ingress,
				Ports: []uint32{1, 2},
				Annotations: map[string]string{
					"test": "value",
				},
			},
			{
				Type:  Ingress,
				Ports: []uint32{3},
				Annotations: map[string]string{
					"test": "value2",
				},
			},
		},
		NodePort: {
			{
				Type:  NodePort,
				Ports: []uint32{4, 5},
			},
		},
	}
	input1 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &IngressServiceConfig{
		Type:  Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value2",
		},
	}
	input3 := &IngressServiceConfig{
		Type:  NodePort,
		Ports: []uint32{4, 5},
	}
	groupedConfig := groupIngressConfig([]*IngressServiceConfig{input1, input2, input3})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_IngressType_Headless(t *testing.T) {
	expected := map[IngressServiceType][]*IngressServiceConfig{
		Headless: {
			{
				Type:  Headless,
				Ports: []uint32{1},
			},
		},
	}
	input := &IngressServiceConfig{
		Type:  Headless,
		Ports: []uint32{1},
	}
	groupedConfig := groupIngressConfig([]*IngressServiceConfig{input})
	assert.Equal(t, groupedConfig, expected)
}

func TestGatherIngressConfigs(t *testing.T) {
	inputConfigs := []*IngressServiceConfig{
		{
			Type:  Ingress,
			Ports: []uint32{1},
		},
		{
			Type:  Ingress,
			Ports: []uint32{2},
		},
		{
			Type:  Headless,
			Ports: []uint32{1},
		},
		{
			Type:  NodePort,
			Ports: []uint32{1},
		},
		{
			Type:  Headless,
			Ports: []uint32{2},
		},
	}

	expected := map[IngressServiceType][]*IngressServiceConfig{
		Ingress: {
			{
				Type:  Ingress,
				Ports: []uint32{1},
			},
			{
				Type:  Ingress,
				Ports: []uint32{2},
			},
		},
		NodePort: {
			{
				Type:  NodePort,
				Ports: []uint32{1},
			},
		},
		Headless: {
			{
				Type:  Headless,
				Ports: []uint32{1},
			},
			{
				Type:  Headless,
				Ports: []uint32{2},
			},
		},
	}

	assert.Equal(t, gatherIngressConfig(inputConfigs), expected)
}

func TestCombineIngressService(t *testing.T) {
	ingress := []*api.IngressConfig{
		{
			Ports: []uint32{1, 2, 3},
			Annotations: map[string]string{
				"Hello": "World",
			},
			TlsEnabled:   true,
			UseClusterIP: false,
		},
	}

	services := []*api.ServiceConfig{
		{
			Type:  api.ServiceType_Headless,
			Ports: []uint32{4},
		},
		{
			Type:  api.ServiceType_NodePort,
			Ports: []uint32{5},
		},
	}

	expected := []*IngressServiceConfig{
		{
			Type:  Ingress,
			Ports: []uint32{1, 2, 3},
			Annotations: map[string]string{
				"Hello": "World",
			},
			TlsEnabled:   true,
			UseClusterIp: false,
		},
		{
			Type:         Headless,
			Ports:        []uint32{4},
			UseClusterIp: false,
		},
		{
			Type:         NodePort,
			Ports:        []uint32{5},
			UseClusterIp: true,
		},
	}

	assert.Equal(t, expected, CombineIngressService(ingress, services))
}
