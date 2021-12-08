package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/pkg/api"
)

func TestDeepCopy(t *testing.T) {
	input := &api.IngressConfig{
		Type:  api.IngressType_NodePort,
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
	config := &api.IngressConfig{
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

	assert.Equal(t, GetServicePorts([]*api.IngressConfig{config}, podSpec), expected)
}

func TestGetServicePorts_MultipleContainer(t *testing.T) {
	config := &api.IngressConfig{
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

	assert.Equal(t, GetServicePorts([]*api.IngressConfig{config}, podSpec), expected)
}

func TestGetServicePorts_MultipleIngressConfigs(t *testing.T) {
	config1 := &api.IngressConfig{
		Ports: []uint32{1},
	}
	config2 := &api.IngressConfig{
		Ports: []uint32{2},
	}
	config3 := &api.IngressConfig{
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
	servicePorts := GetServicePorts([]*api.IngressConfig{config1, config2, config3}, podSpec)
	assert.Equal(t, servicePorts, expected)
}

func TestGetServicePorts_HostPortSkipped(t *testing.T) {
	config := &api.IngressConfig{
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

	assert.Equal(t, GetServicePorts([]*api.IngressConfig{config}, podSpec), expected)
}

func TestGroupIngressConfig_IngressTypeNodePort_AlwaysGrouped(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_NodePort: {
			{
				Type:  api.IngressType_NodePort,
				Ports: []uint32{1, 2, 3},
			},
		},
	}
	input1 := &api.IngressConfig{
		Type:  api.IngressType_NodePort,
		Ports: []uint32{1, 2},
	}
	input2 := &api.IngressConfig{
		Type:  api.IngressType_NodePort,
		Ports: []uint32{3},
	}
	groupedConfig := groupIngressConfig([]*api.IngressConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)

	input2.Annotations = map[string]string{
		"test": "value",
	}
	//Always grouped, regardless of annotations
	assert.Equal(t, groupIngressConfig([]*api.IngressConfig{input1, input2}), expected)
}

func TestGroupIngressConfig_IngressType_NoAnnotations(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_Ingress: {
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{1, 2, 3},
			},
		},
	}
	input1 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{1, 2},
	}
	input2 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{3},
	}
	groupedConfig := groupIngressConfig([]*api.IngressConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_IngressType_SameAnnotations(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_Ingress: {
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{1, 2, 3},
				Annotations: map[string]string{
					"test": "value",
				},
			},
		},
	}
	input1 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	assert.Equal(t, groupIngressConfig([]*api.IngressConfig{input1, input2}), expected)
}

func TestGroupIngressConfig_IngressType_DifferentAnnotations(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_Ingress: {
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{1, 2},
				Annotations: map[string]string{
					"test": "value",
				},
			},
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{3},
				Annotations: map[string]string{
					"test": "value2",
				},
			},
		},
	}
	input1 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value2",
		},
	}
	groupedConfig := groupIngressConfig([]*api.IngressConfig{input1, input2})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_MixedIngressType(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_Ingress: {
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{1, 2},
				Annotations: map[string]string{
					"test": "value",
				},
			},
			{
				Type:  api.IngressType_Ingress,
				Ports: []uint32{3},
				Annotations: map[string]string{
					"test": "value2",
				},
			},
		},
		api.IngressType_NodePort: {
			{
				Type:  api.IngressType_NodePort,
				Ports: []uint32{4, 5},
			},
		},
	}
	input1 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{1, 2},
		Annotations: map[string]string{
			"test": "value",
		},
	}
	input2 := &api.IngressConfig{
		Type:  api.IngressType_Ingress,
		Ports: []uint32{3},
		Annotations: map[string]string{
			"test": "value2",
		},
	}
	input3 := &api.IngressConfig{
		Type:  api.IngressType_NodePort,
		Ports: []uint32{4, 5},
	}
	groupedConfig := groupIngressConfig([]*api.IngressConfig{input1, input2, input3})
	assert.Equal(t, groupedConfig, expected)
}

func TestGroupIngressConfig_IngressType_Headless(t *testing.T) {
	expected := map[api.IngressType][]*api.IngressConfig{
		api.IngressType_Headless: {
			{
				Type:  api.IngressType_Headless,
				Ports: []uint32{1},
				Selector: map[string]string{
					"test": "label",
				},
			},
		},
	}
	input := &api.IngressConfig{
		Type:  api.IngressType_Headless,
		Ports: []uint32{1},
		Selector: map[string]string{
			"test": "label",
		},
	}
	groupedConfig := groupIngressConfig([]*api.IngressConfig{input})
	assert.Equal(t, groupedConfig, expected)
}
