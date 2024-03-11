package conversion

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common"
	"strings"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/api"
)

func CreateService(
	req *api.JobSubmitRequest,
	jobReq *api.JobSubmitRequestItem,
	jobId string,
	owner string,
	ports []v1.ServicePort,
	ingSvcType IngressServiceType,
	useClusterIP bool,
) *v1.Service {
	serviceType := v1.ServiceTypeClusterIP
	if ingSvcType == NodePort {
		serviceType = v1.ServiceTypeNodePort
	}

	clusterIP := ""
	if !useClusterIP {
		clusterIP = "None"
	}

	serviceSpec := v1.ServiceSpec{
		Type: serviceType,
		Selector: map[string]string{
			domain.JobId: jobId,
			domain.Queue: req.Queue,
		},
		Ports:     ports,
		ClusterIP: clusterIP,
	}
	labels := util.MergeMaps(jobReq.Labels, map[string]string{
		domain.JobId: jobId,
		domain.Queue: req.Queue,
	})
	annotation := util.MergeMaps(jobReq.Annotations, map[string]string{
		domain.JobSetId: req.JobSetId,
		domain.Owner:    owner,
	})
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%s", common.PodName(jobId), strings.ToLower(ingSvcType.String())),
			Labels:      labels,
			Annotations: annotation,
			Namespace:   jobReq.Namespace,
		},
		Spec: serviceSpec,
	}
	return service
}

func CreateIngress(
	name string,
	req *api.JobSubmitRequest,
	jobReq *api.JobSubmitRequestItem,
	jobId string,
	owner string,
	service *v1.Service,
	jobConfig *IngressServiceConfig,
) *networking.Ingress {
	labels := util.MergeMaps(jobReq.Labels, map[string]string{
		domain.JobId: jobId,
		domain.Queue: req.Queue,
	})
	annotations := util.MergeMaps(jobReq.Annotations, jobConfig.Annotations)
	annotations = util.MergeMaps(annotations, map[string]string{
		domain.JobSetId: req.JobSetId,
		domain.Owner:    owner,
	})

	rules := make([]networking.IngressRule, 0, len(service.Spec.Ports))
	tlsHosts := make([]string, 0, len(service.Spec.Ports))

	// Rest of the hosts are generated off port information
	for _, servicePort := range service.Spec.Ports {
		if !contains(jobConfig, uint32(servicePort.Port)) {
			continue
		}
		host := fmt.Sprintf("%s-%s.%s.%s", servicePort.Name, common.PodName(jobId), jobReq.Namespace, "")
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
									Name: service.Name,
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

	if jobConfig.TlsEnabled {
		certName := jobConfig.CertName
		if certName == "" {
			certName = fmt.Sprintf("%s-%s", jobReq.Namespace, "")
		}

		tls = append(tls, networking.IngressTLS{
			Hosts:      tlsHosts,
			SecretName: certName,
		})
	}

	ingress := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
			Namespace:   jobReq.Namespace,
		},
		Spec: networking.IngressSpec{
			Rules: rules,
			TLS:   tls,
		},
	}
	return ingress
}
