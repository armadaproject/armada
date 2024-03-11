package conversion

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common"
	"strings"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

func GenerateIngresses(req *api.JobSubmitRequest, jobReq *api.JobSubmitRequestItem, jobId string, owner string) ([]*v1.Service, []*networking.Ingress) {
	var services []*v1.Service
	var ingresses []*networking.Ingress
	ingressToGen := CombineIngressService(jobReq.Ingress, jobReq.Services)
	groupedIngressConfigs := groupIngressConfig(ingressToGen)
	podSpec := jobReq.GetMainPodSpec()
	for svcType, configs := range groupedIngressConfigs {
		if len(GetServicePorts(configs, podSpec)) > 0 {
			service := CreateService(req, jobReq, jobId, owner, GetServicePorts(configs, podSpec), svcType, useClusterIP(configs))
			services = append(services, service)

			if svcType == Ingress {
				for index, config := range configs {
					if len(GetServicePorts([]*IngressServiceConfig{config}, podSpec)) <= 0 {
						continue
					}
					ingressName := fmt.Sprintf("%s-%s-%d", common.PodName(jobId), strings.ToLower(svcType.String()), index)
					ingress := CreateIngress(ingressName, req, jobReq, jobId, owner, service, config)
					ingresses = append(ingresses, ingress)
				}
			}
		}
	}

	return services, ingresses
}

func groupIngressConfig(configs []*IngressServiceConfig) map[IngressServiceType][]*IngressServiceConfig {
	result := gatherIngressConfig(configs)

	for ingressType, grp := range result {
		result[ingressType] = mergeOnAnnotations(grp)
	}

	return result
}

// gatherIngressConfig takes a list of ingress configs and groups them by IngressServiceType
func gatherIngressConfig(configs []*IngressServiceConfig) map[IngressServiceType][]*IngressServiceConfig {
	result := make(map[IngressServiceType][]*IngressServiceConfig, 10)

	for _, config := range configs {
		result[config.Type] = append(result[config.Type], deepCopy(config))
	}

	return result
}

func mergeOnAnnotations(configs []*IngressServiceConfig) []*IngressServiceConfig {
	result := make([]*IngressServiceConfig, 0, len(configs))

	for _, config := range configs {
		matchFound := false

		for _, existingConfig := range result {
			if util.Equal(config.Annotations, existingConfig.Annotations) {
				existingConfig.Ports = append(existingConfig.Ports, config.Ports...)
				matchFound = true
			}
		}
		if !matchFound {
			result = append(result, deepCopy(config))
		}
	}

	return result
}

func GetServicePorts(svcConfigs []*IngressServiceConfig, podSpec *v1.PodSpec) []v1.ServicePort {
	var servicePorts []v1.ServicePort

	for _, container := range podSpec.Containers {
		ports := container.Ports
		for _, svcConfig := range svcConfigs {
			for _, port := range ports {
				// Don't expose host via service, this will already be handled by kubernetes
				if port.HostPort > 0 {
					continue
				}
				if contains(svcConfig, uint32(port.ContainerPort)) {
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

func contains(portConfig *IngressServiceConfig, port uint32) bool {
	for _, p := range portConfig.Ports {
		if p == port {
			return true
		}
	}
	return false
}
