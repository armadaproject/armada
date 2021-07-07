package job

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/pkg/api"
)

const admissionWebhookValidationFailureMessage string = "admission webhook"

type Submitter interface {
	SubmitJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails
}

type SubmitService struct {
	eventReporter  reporter.EventReporter
	clusterContext context.ClusterContext
	podDefaults    *configuration.PodDefaults
}

func NewSubmitter(
	clusterContext context.ClusterContext,
	podDefaults *configuration.PodDefaults) *SubmitService {

	return &SubmitService{
		clusterContext: clusterContext,
		podDefaults:    podDefaults}
}

type FailedSubmissionDetails struct {
	Pod         *v1.Pod
	Job         *api.Job
	Error       error
	Recoverable bool
}

func (allocationService *SubmitService) SubmitJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails {
	toBeFailedJobs := make([]*FailedSubmissionDetails, 0, 10)
	for _, job := range jobsToSubmit {
		jobPods := []*v1.Pod{}
		for i, _ := range job.GetAllPodSpecs() {
			pod, err := allocationService.submitPod(job, i)
			jobPods = append(jobPods, pod)

			if err != nil {
				log.Errorf("Failed to submit job %s because %s", job.Id, err)

				status, ok := err.(errors.APIStatus)
				recoverable := !ok || isNotRecoverable(status.Status())

				errDetails := &FailedSubmissionDetails{
					Job:         job,
					Pod:         pod,
					Error:       err,
					Recoverable: recoverable,
				}

				toBeFailedJobs = append(toBeFailedJobs, errDetails)

				// remove just created pods
				allocationService.clusterContext.DeletePods(jobPods)
				break
			}
		}
	}

	return toBeFailedJobs
}

func (allocationService *SubmitService) submitPod(job *api.Job, i int) (*v1.Pod, error) {
	pod := createPod(job, allocationService.podDefaults, i)

	if exposesPorts(job, &pod.Spec) {
		groupedIngressConfigs := groupIngressConfig(job.Ingress)
		count := 0
		for ingressType, configs := range groupedIngressConfigs {
			if ingressType == api.IngressType_Ingress {
				count += len(configs)
			}
		}
		pod.Annotations = MergeMaps(pod.Annotations, map[string]string{
			domain.HasIngress:               "true",
			domain.AssociatedIngressesCount: fmt.Sprintf("%d", count),
			domain.AssociatedServicesCount:  fmt.Sprintf("%d", len(groupedIngressConfigs)),
		})
		submittedPod, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		if err != nil {
			return pod, err
		}
		for ingressType, configs := range groupedIngressConfigs {
			if len(getServicePorts(configs, &pod.Spec)) > 0 {
				service := createService(job, submittedPod, getServicePorts(configs, &pod.Spec), ingressType)
				_, err = allocationService.clusterContext.SubmitService(service)
				if err != nil {
					return pod, err
				}
				if ingressType == api.IngressType_Ingress {
					for index, config := range configs {
						if len(getServicePorts([]*api.IngressConfig{config}, &pod.Spec)) <= 0 {
							continue
						}
						ingressName := fmt.Sprintf("%s-%s-%d", pod.Name, strings.ToLower(ingressType.String()), index)
						ingress := createIngress(ingressName, job, submittedPod, service, allocationService.podDefaults.Ingress, config)
						_, err = allocationService.clusterContext.SubmitIngress(ingress)
						if err != nil {
							return pod, err
						}
					}
				}
			}
		}
		return pod, err
	} else {
		_, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		return pod, err
	}
}

func groupIngressConfig(configs []*api.IngressConfig) map[api.IngressType][]*api.IngressConfig {
	result := make(map[api.IngressType][]*api.IngressConfig, 10)

	for _, config := range configs {
		if _, present := result[config.Type]; !present {
			result[config.Type] = []*api.IngressConfig{deepcopy(config)}
			continue
		}

		existingConfigsOfType := result[config.Type]
		if config.Type == api.IngressType_NodePort {
			existingConfigsOfType[0].Ports = append(existingConfigsOfType[0].Ports, config.Ports...)
		} else {
			matchFound := false
			for _, existingConfig := range existingConfigsOfType {
				if isStringMapEqual(config.Annotations, existingConfig.Annotations) {
					existingConfig.Ports = append(existingConfig.Ports, config.Ports...)
					matchFound = true
				}
			}
			if !matchFound {
				result[config.Type] = append(existingConfigsOfType, deepcopy(config))
			}
		}
	}
	return result
}

func deepcopy(config *api.IngressConfig) *api.IngressConfig {
	return &api.IngressConfig{
		Type:        config.GetType(),
		Ports:       config.Ports,
		Annotations: config.Annotations,
	}
}

func isMetadataEqual(a *api.IngressConfig, b *api.IngressConfig) bool {
	return isStringMapEqual(a.Annotations, b.Annotations)
}

func isStringMapEqual(a map[string]string, b map[string]string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}

	if len(a) != len(b) {
		return false
	}

	for key, value := range a {
		if comparativeValue, present := b[key]; !present || value != comparativeValue {
			return false
		}
	}
	return true
}

func getServicePorts(ingressConfigs []*api.IngressConfig, podSpec *v1.PodSpec) []v1.ServicePort {
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

func exposesPorts(job *api.Job, podSpec *v1.PodSpec) bool {
	return len(getServicePorts(job.Ingress, podSpec)) > 0
}

func isNotRecoverable(status metav1.Status) bool {
	if status.Reason == metav1.StatusReasonInvalid ||
		status.Reason == metav1.StatusReasonForbidden {
		return true
	}

	//This message shows it was rejected by an admission webhook.
	// By default admission webhooks blocking results in a 500 so we can't use the status code as we could confuse it with Kubernetes outage
	if strings.Contains(status.Message, admissionWebhookValidationFailureMessage) {
		return true
	}

	return false
}

func createService(job *api.Job, pod *v1.Pod, ports []v1.ServicePort, ingressType api.IngressType) *v1.Service {
	ownerReference := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}
	serviceType := v1.ServiceTypeClusterIP
	if ingressType == api.IngressType_NodePort {
		serviceType = v1.ServiceTypeNodePort
	}
	serviceSpec := v1.ServiceSpec{
		Type: serviceType,
		Selector: map[string]string{
			domain.JobId:     pod.Labels[domain.JobId],
			domain.Queue:     pod.Labels[domain.Queue],
			domain.PodNumber: pod.Labels[domain.PodNumber],
		},
		Ports: ports,
	}
	labels := MergeMaps(job.Labels, map[string]string{
		domain.JobId:     pod.Labels[domain.JobId],
		domain.Queue:     pod.Labels[domain.Queue],
		domain.PodNumber: pod.Labels[domain.PodNumber],
	})
	annotation := MergeMaps(job.Annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-%s", pod.Name, strings.ToLower(ingressType.String())),
			Labels:          labels,
			Annotations:     annotation,
			Namespace:       job.Namespace,
			OwnerReferences: []metav1.OwnerReference{ownerReference},
		},
		Spec: serviceSpec,
	}
	return service
}

func createIngress(name string, job *api.Job, pod *v1.Pod, service *v1.Service, executorIngressConfig *configuration.IngressConfiguration, jobConfig *api.IngressConfig) *networking.Ingress {
	ownerReference := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}

	labels := MergeMaps(job.Labels, map[string]string{
		domain.JobId:     pod.Labels[domain.JobId],
		domain.Queue:     pod.Labels[domain.Queue],
		domain.PodNumber: pod.Labels[domain.PodNumber],
	})
	annotation := MergeMaps(job.Annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})
	annotation = MergeMaps(annotation, executorIngressConfig.Annotations)
	annotation = MergeMaps(annotation, jobConfig.Annotations)

	rules := make([]networking.IngressRule, 0, len(service.Spec.Ports))
	for _, servicePort := range service.Spec.Ports {
		if !contains(jobConfig, uint32(servicePort.Port)) {
			continue
		}
		path := networking.IngressRule{
			Host: fmt.Sprintf("%s.%s.%s.%s", servicePort.Name, pod.Name, pod.Namespace, executorIngressConfig.HostnameSuffix),
			IngressRuleValue: networking.IngressRuleValue{
				HTTP: &networking.HTTPIngressRuleValue{
					Paths: []networking.HTTPIngressPath{
						{
							Path: "/",
							Backend: networking.IngressBackend{
								ServiceName: service.Name,
								ServicePort: intstr.IntOrString{IntVal: servicePort.Port},
							},
						},
					},
				},
			},
		}
		rules = append(rules, path)
	}

	ingress := &networking.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Labels:          labels,
			Annotations:     annotation,
			Namespace:       job.Namespace,
			OwnerReferences: []metav1.OwnerReference{ownerReference},
		},
		Spec: networking.IngressSpec{
			Rules: rules,
		},
	}
	return ingress
}

func createPod(job *api.Job, defaults *configuration.PodDefaults, i int) *v1.Pod {

	allPodSpecs := job.GetAllPodSpecs()
	podSpec := allPodSpecs[i]
	applyDefaults(podSpec, defaults)

	labels := MergeMaps(job.Labels, map[string]string{
		domain.JobId:     job.Id,
		domain.Queue:     job.Queue,
		domain.PodNumber: strconv.Itoa(i),
		domain.PodCount:  strconv.Itoa(len(allPodSpecs)),
	})
	annotation := MergeMaps(job.Annotations, map[string]string{
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

func MergeMaps(a map[string]string, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}
