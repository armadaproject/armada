package util

import (
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/api"
)

func CreateService(
	job *api.Job,
	pod *v1.Pod,
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
			domain.JobId:     pod.Labels[domain.JobId],
			domain.Queue:     pod.Labels[domain.Queue],
			domain.PodNumber: pod.Labels[domain.PodNumber],
		},
		Ports:     ports,
		ClusterIP: clusterIP,
	}
	labels := util.MergeMaps(job.Labels, map[string]string{
		domain.JobId:     pod.Labels[domain.JobId],
		domain.Queue:     pod.Labels[domain.Queue],
		domain.PodNumber: pod.Labels[domain.PodNumber],
	})
	annotation := util.MergeMaps(job.Annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%s", pod.Name, strings.ToLower(ingSvcType.String())),
			Labels:      labels,
			Annotations: annotation,
			Namespace:   job.Namespace,
		},
		Spec: serviceSpec,
	}
	return service
}

func CreateIngress(
	name string,
	job *api.Job,
	pod *v1.Pod,
	service *v1.Service,
	executorIngressConfig *configuration.IngressConfiguration,
	jobConfig *IngressServiceConfig,
) *networking.Ingress {
	labels := util.MergeMaps(job.Labels, map[string]string{
		domain.JobId:     pod.Labels[domain.JobId],
		domain.Queue:     pod.Labels[domain.Queue],
		domain.PodNumber: pod.Labels[domain.PodNumber],
	})
	annotations := util.MergeMaps(job.Annotations, executorIngressConfig.Annotations)
	annotations = util.MergeMaps(annotations, jobConfig.Annotations)
	annotations = util.MergeMaps(annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})

	rules := make([]networking.IngressRule, 0, len(service.Spec.Ports))
	tlsHosts := make([]string, 0, len(service.Spec.Ports))

	// Rest of the hosts are generated off port information
	for _, servicePort := range service.Spec.Ports {
		if !contains(jobConfig, uint32(servicePort.Port)) {
			continue
		}
		host := fmt.Sprintf("%s-%s.%s.%s", servicePort.Name, pod.Name, pod.Namespace, executorIngressConfig.HostnameSuffix)
		tlsHosts = append(tlsHosts, host)

		// Workaround to get constant's address
		pathType := networking.PathTypeImplementationSpecific
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
			certName = fmt.Sprintf("%s-%s", job.Namespace, executorIngressConfig.CertNameSuffix)
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
			Namespace:   job.Namespace,
		},
		Spec: networking.IngressSpec{
			Rules: rules,
			TLS:   tls,
		},
	}
	return ingress
}

func CreateOwnerReference(pod *v1.Pod) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}
}

func CreatePod(job *api.Job, defaults *configuration.PodDefaults, i int) *v1.Pod {
	allPodSpecs := job.GetAllPodSpecs()
	podSpec := allPodSpecs[i]
	applyDefaults(podSpec, defaults)

	labels := util.MergeMaps(job.Labels, map[string]string{
		domain.JobId:     job.Id,
		domain.Queue:     job.Queue,
		domain.PodNumber: strconv.Itoa(i),
		domain.PodCount:  strconv.Itoa(len(allPodSpecs)),
	})
	annotation := util.MergeMaps(job.Annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})

	setRestartPolicyNever(podSpec)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        common.PodNamePrefix + job.Id + "-" + strconv.Itoa(i),
			Labels:      labels,
			Annotations: annotation,
			Namespace:   job.Namespace,
		},
		Spec: *podSpec,
	}

	return pod
}

func applyDefaults(spec *v1.PodSpec, defaults *configuration.PodDefaults) {
	if defaults == nil {
		return
	}
	if defaults.SchedulerName != "" && spec.SchedulerName == "" {
		spec.SchedulerName = defaults.SchedulerName
	}
}

func setRestartPolicyNever(podSpec *v1.PodSpec) {
	podSpec.RestartPolicy = v1.RestartPolicyNever
}
