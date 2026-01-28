package conversion

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/common"
	log "github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// SubmitJobFromApiRequest converts an *api.JobSubmitRequest into an *armadaevents.SubmitJob
// It is assumed that the jobReq has already been validated to ensure that all necessary data is present
func SubmitJobFromApiRequest(
	jobReq *api.JobSubmitRequestItem,
	config configuration.SubmissionConfig,
	jobSetId, queue, owner string,
	idGen func() string, //  injected so that ids can be stable for testing
) *armadaevents.SubmitJob {
	jobId := idGen()
	priority := PriorityAsInt32(jobReq.GetPriority())
	ingressesAndServices := convertIngressesAndServices(config, jobReq, jobId, jobSetId, queue, owner)

	msg := &armadaevents.SubmitJob{
		JobId:           jobId,
		DeduplicationId: jobReq.GetClientId(),
		Priority:        priority,
		ObjectMeta: &armadaevents.ObjectMeta{
			Namespace:   jobReq.GetNamespace(),
			Annotations: jobReq.GetAnnotations(),
			Labels:      jobReq.GetLabels(),
		},
		MainObject: &armadaevents.KubernetesMainObject{
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: jobReq.GetMainPodSpec(),
				},
			},
		},
		Objects:   ingressesAndServices,
		Scheduler: jobReq.Scheduler,
	}

	postProcess(msg, config)
	return msg
}

// Creates KubernetesObjects representing ingresses and services from the *api.JobSubmitRequestItem.
// An ingress will have a corresponding service created for it.
func convertIngressesAndServices(
	config configuration.SubmissionConfig,
	jobReq *api.JobSubmitRequestItem,
	jobId, jobsetId, queue, owner string,
) []*armadaevents.KubernetesObject {
	objects := make([]*armadaevents.KubernetesObject, 0, 2*len(jobReq.Ingress)+len(jobReq.Services))
	serviceIdx := 0
	ingressIdx := 0

	// Extract Ports from main containers and native sidecar containers
	availableServicePorts := make([]v1.ServicePort, 0)

	// Extract from main containers
	for _, container := range jobReq.GetMainPodSpec().Containers {
		for _, port := range container.Ports {
			// Don't expose host via service, this will already be handled by kubernetes
			if port.HostPort > 0 {
				continue
			}
			availableServicePorts = append(availableServicePorts, v1.ServicePort{
				Name:     fmt.Sprintf("%s-%d", container.Name, port.ContainerPort),
				Port:     port.ContainerPort,
				Protocol: port.Protocol,
			})
		}
	}

	// Extract from native sidecar init containers (restartPolicy: Always)
	// Classic init containers (without restartPolicy) run to completion before
	// main containers start, so their ports shouldn't be exposed.
	for _, container := range jobReq.GetMainPodSpec().InitContainers {
		// Skip classic init containers
		if container.RestartPolicy == nil || *container.RestartPolicy != v1.ContainerRestartPolicyAlways {
			continue
		}

		for _, port := range container.Ports {
			// Don't expose host via service, this will already be handled by kubernetes
			if port.HostPort > 0 {
				continue
			}
			availableServicePorts = append(availableServicePorts, v1.ServicePort{
				Name:     fmt.Sprintf("%s-%d", container.Name, port.ContainerPort),
				Port:     port.ContainerPort,
				Protocol: port.Protocol,
			})
		}
	}

	// Create standalone services first
	createdServices := make([]*armadaevents.KubernetesObject, 0)
	for _, serviceConfig := range jobReq.Services {
		ports := filterServicePorts(availableServicePorts, serviceConfig.Ports)
		if len(ports) > 0 {
			serviceType := v1.ServiceTypeClusterIP
			useClusterIp := false
			if serviceConfig.Type == api.ServiceType_NodePort {
				serviceType = v1.ServiceTypeNodePort
				useClusterIp = true
			}

			serviceName := fmt.Sprintf("%s-service-%d", common.PodName(jobId), serviceIdx)
			serviceNameCustomized := false

			if len(serviceConfig.Name) > 0 {
				if config.AllowCustomServiceNames {
					serviceName = serviceConfig.Name
					serviceNameCustomized = true
				} else {
					log.Warnf("Feature flag to allow customized service name %s is not enabled. "+
						"Using generated service name %s", serviceConfig.Name, serviceName)
				}
			}

			if !serviceNameCustomized {
				serviceIdx++
			}

			serviceObject := createService(serviceName, jobId, ports, serviceType, useClusterIp)
			createdServices = append(createdServices, serviceObject)
			objects = append(objects, serviceObject)
		}
	}

	// Create ingresses, reusing existing services when possible
	for _, ingressConfig := range jobReq.Ingress {
		ingressPorts := filterServicePorts(availableServicePorts, ingressConfig.Ports)
		if len(ingressPorts) == 0 {
			continue
		}

		// Check if any existing service covers the ingress port(s)
		var targetService *armadaevents.KubernetesObject
		for _, service := range createdServices {
			servicePorts := service.GetService().Ports
			if containsAllPorts(servicePorts, ingressPorts) {
				targetService = service
				break
			}
		}

		if targetService != nil {
			// Reuse existing service - create only the ingress
			ingressObject := createIngressFromService(
				targetService.GetService(),
				ingressIdx,
				ingressConfig,
				targetService.ObjectMeta.Name,
				jobReq.Namespace,
				jobId)
			objects = append(objects, ingressObject)
			ingressIdx++
		} else {
			// No suitable service exists - create both service and ingress
			serviceName := fmt.Sprintf("%s-service-%d", common.PodName(jobId), serviceIdx)
			serviceObject := createService(serviceName, jobId, ingressPorts, v1.ServiceTypeClusterIP, ingressConfig.UseClusterIP)
			serviceIdx++
			ingressObject := createIngressFromService(
				serviceObject.GetService(),
				ingressIdx,
				ingressConfig,
				serviceObject.ObjectMeta.Name,
				jobReq.Namespace,
				jobId)
			objects = append(objects, serviceObject)
			objects = append(objects, ingressObject)
			ingressIdx++
		}
	}

	// Add standard annotations and labels to all objects
	for _, object := range objects {
		md := object.GetObjectMeta()
		md.Namespace = jobReq.Namespace

		annotations := md.GetAnnotations()
		annotations[domain.JobSetId] = jobsetId
		annotations[domain.Owner] = owner

		labels := md.GetLabels()
		labels[domain.JobId] = jobId
		labels[domain.Queue] = queue
	}

	return objects
}

func createService(
	serviceName string,
	jobId string,
	ports []v1.ServicePort,
	serviceType v1.ServiceType,
	useClusterIP bool,
) *armadaevents.KubernetesObject {
	var clusterIP string
	if useClusterIP {
		clusterIP = ""
	} else {
		clusterIP = "None"
	}

	return &armadaevents.KubernetesObject{
		ObjectMeta: &armadaevents.ObjectMeta{
			Name:        serviceName,
			Annotations: map[string]string{},
			Labels:      map[string]string{},
		},
		Object: &armadaevents.KubernetesObject_Service{
			Service: &v1.ServiceSpec{
				Type: serviceType,
				Selector: map[string]string{
					domain.JobId: jobId,
				},
				Ports:     ports,
				ClusterIP: clusterIP,
			},
		},
	}
}

func createIngressFromService(
	service *v1.ServiceSpec,
	serviceIdx int,
	ingressConfig *api.IngressConfig,
	serviceName, namespace, jobId string,
) *armadaevents.KubernetesObject {
	// Use specified ingress ports, or all service ports if none specified (legacy behavior)
	ingressPorts := service.Ports
	if len(ingressConfig.Ports) > 0 {
		ingressPorts = filterServicePorts(service.Ports, ingressConfig.Ports)
	}

	rules := make([]networking.IngressRule, 0, len(ingressPorts))
	tlsHosts := make([]string, 0, len(ingressPorts))

	// Create ingress rules only for the specified ingress ports
	for _, servicePort := range ingressPorts {
		host := fmt.Sprintf("%s-%s.%s.", servicePort.Name, common.PodName(jobId), namespace)
		tlsHosts = append(tlsHosts, host)

		// Workaround to get constant's address
		pathType := networking.PathTypePrefix
		path := networking.IngressRule{
			Host: host,
			IngressRuleValue: networking.IngressRuleValue{
				HTTP: &networking.HTTPIngressRuleValue{
					Paths: []networking.HTTPIngressPath{
						{
							Path:     "/",
							PathType: &pathType,
							Backend: networking.IngressBackend{
								Service: &networking.IngressServiceBackend{
									Name: serviceName,
									Port: networking.ServiceBackendPort{
										Number: servicePort.Port,
									},
								},
							},
						},
					},
				},
			},
		}
		rules = append(rules, path)
	}

	tls := make([]networking.IngressTLS, 0, 1)

	if ingressConfig.TlsEnabled {
		certName := ingressConfig.CertName
		if certName == "" {
			certName = fmt.Sprintf("%s-", namespace)
		}
		tls = append(tls, networking.IngressTLS{
			Hosts:      tlsHosts,
			SecretName: certName,
		})
	}

	return &armadaevents.KubernetesObject{
		ObjectMeta: &armadaevents.ObjectMeta{
			Name:        fmt.Sprintf("%s-ingress-%d", common.PodName(jobId), serviceIdx),
			Annotations: util.MergeMaps(map[string]string{}, ingressConfig.Annotations),
			Labels:      map[string]string{},
		},
		Object: &armadaevents.KubernetesObject_Ingress{
			Ingress: &networking.IngressSpec{
				Rules: rules,
				TLS:   tls,
			},
		},
	}
}

func PriorityAsInt32(priority float64) uint32 {
	if priority < 0 {
		priority = 0
	}
	if priority > math.MaxUint32 {
		priority = math.MaxUint32
	}
	priority = math.Round(priority)
	return uint32(priority)
}

func filterServicePorts(availablePorts []v1.ServicePort, desiredPorts []uint32) []v1.ServicePort {
	return armadaslices.Filter(availablePorts, func(availablePort v1.ServicePort) bool {
		for _, desiredPort := range desiredPorts {
			if availablePort.Port == int32(desiredPort) {
				return true
			}
		}
		return false
	})
}

// containsAllPorts checks if servicePorts contains all ports from requiredPorts
func containsAllPorts(servicePorts []v1.ServicePort, requiredPorts []v1.ServicePort) bool {
	for _, requiredPort := range requiredPorts {
		found := false
		for _, servicePort := range servicePorts {
			if servicePort.Port == requiredPort.Port {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
