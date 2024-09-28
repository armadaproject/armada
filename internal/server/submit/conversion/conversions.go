package conversion

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/common"
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
	ingressesAndServices := convertIngressesAndServices(jobReq, jobId, jobSetId, queue, owner)

	msg := &armadaevents.SubmitJob{
		JobIdStr:        jobId,
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
// An ingress will have  a corresponding service created for it.
func convertIngressesAndServices(
	jobReq *api.JobSubmitRequestItem,
	jobId, jobsetId, queue, owner string,
) []*armadaevents.KubernetesObject {
	objects := make([]*armadaevents.KubernetesObject, 0, 2*len(jobReq.Ingress)+len(jobReq.Services))
	serviceIdx := 0

	// Extract Ports from containers
	availableServicePorts := make([]v1.ServicePort, 0)
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

	// Create ingress and associated services
	for _, ingressConfig := range jobReq.Ingress {
		ports := filterServicePorts(availableServicePorts, ingressConfig.Ports)
		if len(ports) > 0 {
			serviceObject := createService(jobId, serviceIdx, ports, v1.ServiceTypeClusterIP, ingressConfig.UseClusterIP)
			serviceIdx++
			ingressObject := createIngressFromService(
				serviceObject.GetService(),
				serviceIdx,
				ingressConfig,
				serviceObject.ObjectMeta.Name,
				jobReq.Namespace,
				jobId)
			objects = append(objects, serviceObject)
			objects = append(objects, ingressObject)
		}
	}

	// Create standalone services
	for _, serviceConfig := range jobReq.Services {
		ports := filterServicePorts(availableServicePorts, serviceConfig.Ports)
		if len(ports) > 0 {
			serviceType := v1.ServiceTypeClusterIP
			useClusterIp := false
			if serviceConfig.Type == api.ServiceType_NodePort {
				serviceType = v1.ServiceTypeNodePort
				useClusterIp = true
			}
			serviceObject := createService(jobId, serviceIdx, ports, serviceType, useClusterIp)
			serviceIdx++
			objects = append(objects, serviceObject)
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
	jobId string,
	serviceIdx int,
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
			Name:        fmt.Sprintf("%s-service-%d", common.PodName(jobId), serviceIdx),
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
	rules := make([]networking.IngressRule, 0, len(service.Ports))
	tlsHosts := make([]string, 0, len(service.Ports))

	// Rest of the hosts are generated off port information
	for _, servicePort := range service.Ports {
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
