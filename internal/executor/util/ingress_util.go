package util

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/pkg/api"
)

func GenerateIngresses(job *api.Job, pod *v1.Pod, ingressConfig *configuration.IngressConfiguration) ([]*v1.Service, []*networking.Ingress) {
	services := []*v1.Service{}
	ingresses := []*networking.Ingress{}

	groupedIngressConfigs := groupIngressConfig(job.Ingress)
	for ingressType, configs := range groupedIngressConfigs {
		if len(GetServicePorts(configs, &pod.Spec)) > 0 {
			service := CreateService(job, pod, GetServicePorts(configs, &pod.Spec), ingressType)
			services = append(services, service)

			if ingressType == api.IngressType_Ingress {
				for index, config := range configs {
					if len(GetServicePorts([]*api.IngressConfig{config}, &pod.Spec)) <= 0 {
						continue
					}
					ingressName := fmt.Sprintf("%s-%s-%d", pod.Name, strings.ToLower(ingressType.String()), index)
					ingress := CreateIngress(ingressName, job, pod, service, ingressConfig, config)
					ingresses = append(ingresses, ingress)
				}
			}
		}
	}

	return services, ingresses
}

func groupIngressConfig(configs []*api.IngressConfig) map[api.IngressType][]*api.IngressConfig {
	result := make(map[api.IngressType][]*api.IngressConfig, 10)

	for _, config := range configs {
		if _, present := result[config.Type]; !present {
			result[config.Type] = []*api.IngressConfig{deepCopy(config)}
			continue
		}

		existingConfigsOfType := result[config.Type]
		if config.Type == api.IngressType_NodePort {
			existingConfigsOfType[0].Ports = append(existingConfigsOfType[0].Ports, config.Ports...)
		} else {
			matchFound := false
			for _, existingConfig := range existingConfigsOfType {
				if util.Equal(config.Annotations, existingConfig.Annotations) {
					existingConfig.Ports = append(existingConfig.Ports, config.Ports...)
					matchFound = true
				}
			}
			if !matchFound {
				result[config.Type] = append(existingConfigsOfType, deepCopy(config))
			}
		}
	}
	return result
}

func deepCopy(config *api.IngressConfig) *api.IngressConfig {
	return &api.IngressConfig{
		Type:        config.GetType(),
		Ports:       util.DeepCopyListUint32(config.Ports),
		Annotations: util.DeepCopy(config.Annotations),
		TlsEnabled:  config.TlsEnabled,
		CertName:    config.CertName,
	}
}

func GetServicePorts(ingressConfigs []*api.IngressConfig, podSpec *v1.PodSpec) []v1.ServicePort {
	var servicePorts []v1.ServicePort

	for _, container := range podSpec.Containers {
		ports := container.Ports
		for _, ingressConfig := range ingressConfigs {
			for _, port := range ports {
				//Don't expose host via service, this will already be handled by kubernetes
				if port.HostPort > 0 {
					continue
				}
				if contains(ingressConfig, uint32(port.ContainerPort)) {
					servicePort := v1.ServicePort{
						Name:     fmt.Sprintf("%s-%d", container.Name, port.ContainerPort),
						Port:     port.ContainerPort,
						Protocol: port.Protocol,
					}
					servicePorts = append(servicePorts, servicePort)
				}
			}
		}
	}

	return servicePorts
}

func contains(portConfig *api.IngressConfig, port uint32) bool {
	for _, p := range portConfig.Ports {
		if p == port {
			return true
		}
	}
	return false
}
