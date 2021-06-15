package job

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		pod.Annotations = mergeMaps(pod.Annotations, map[string]string{
			domain.HasIngress: "true",
		})
		submittedPod, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		if err != nil {
			return pod, err
		}
		service := createService(job, submittedPod)
		_, err = allocationService.clusterContext.SubmitService(service)
		return pod, err
	} else {
		_, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		return pod, err
	}
}

func getServicePorts(job *api.Job, podSpec *v1.PodSpec) []v1.ServicePort {
	var servicePorts []v1.ServicePort
	if job.Ingress == nil || len(job.Ingress) == 0 {
		return servicePorts
	}

	for _, container := range podSpec.Containers {
		ports := container.Ports
		for _, port := range ports {
			//Don't expose host via service, this will already be handled by kubernetes
			if port.HostPort > 0 {
				continue
			}
			if shouldExposeWithNodePort(port, job) {
				servicePort := v1.ServicePort{
					Name:     fmt.Sprintf("%s-%d", container.Name, port.ContainerPort),
					Port:     port.ContainerPort,
					Protocol: port.Protocol,
				}

				servicePorts = append(servicePorts, servicePort)
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

func shouldExposeWithNodePort(port v1.ContainerPort, job *api.Job) bool {
	for _, ingressConfig := range job.Ingress {
		if ingressConfig.Type == api.IngressType_NodePort &&
			contains(ingressConfig, uint32(port.ContainerPort)) {
			return true
		}
	}
	return false
}

func exposesPorts(job *api.Job, podSpec *v1.PodSpec) bool {
	return len(getServicePorts(job, podSpec)) > 0
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

func createService(job *api.Job, pod *v1.Pod) *v1.Service {
	servicePorts := getServicePorts(job, &pod.Spec)

	ownerReference := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}
	serviceSpec := v1.ServiceSpec{
		Type: v1.ServiceTypeNodePort,
		Selector: map[string]string{
			domain.JobId:     pod.Labels[domain.JobId],
			domain.Queue:     pod.Labels[domain.Queue],
			domain.PodNumber: pod.Labels[domain.PodNumber],
		},
		Ports: servicePorts,
	}
	labels := mergeMaps(job.Labels, map[string]string{
		domain.JobId:     pod.Labels[domain.JobId],
		domain.Queue:     pod.Labels[domain.Queue],
		domain.PodNumber: pod.Labels[domain.PodNumber],
	})
	annotation := mergeMaps(job.Annotations, map[string]string{
		domain.JobSetId: job.JobSetId,
		domain.Owner:    job.Owner,
	})
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pod.Name,
			Labels:          labels,
			Annotations:     annotation,
			Namespace:       job.Namespace,
			OwnerReferences: []metav1.OwnerReference{ownerReference},
		},
		Spec: serviceSpec,
	}
	return service
}

func createPod(job *api.Job, defaults *configuration.PodDefaults, i int) *v1.Pod {

	allPodSpecs := job.GetAllPodSpecs()
	podSpec := allPodSpecs[i]
	applyDefaults(podSpec, defaults)

	labels := mergeMaps(job.Labels, map[string]string{
		domain.JobId:     job.Id,
		domain.Queue:     job.Queue,
		domain.PodNumber: strconv.Itoa(i),
		domain.PodCount:  strconv.Itoa(len(allPodSpecs)),
	})
	annotation := mergeMaps(job.Annotations, map[string]string{
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

func mergeMaps(a map[string]string, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}
