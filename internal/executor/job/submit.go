package job

import (
	"fmt"
	"strings"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/pkg/api"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	pod := CreatePod(job, allocationService.podDefaults, i)

	if exposesPorts(job, &pod.Spec) {
		services, ingresses := GenerateIngresses(job, pod, allocationService.podDefaults.Ingress)
		pod.Annotations = util.MergeMaps(pod.Annotations, map[string]string{
			domain.HasIngress:               "true",
			domain.AssociatedServicesCount:  fmt.Sprintf("%d", len(services)),
			domain.AssociatedIngressesCount: fmt.Sprintf("%d", len(ingresses)),
		})
		submittedPod, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		if err != nil {
			return pod, err
		}
		for _, service := range services {
			service.ObjectMeta.OwnerReferences = []metav1.OwnerReference{CreateOwnerReference(submittedPod)}
			_, err = allocationService.clusterContext.SubmitService(service)
			if err != nil {
				return pod, err
			}
		}
		for _, ingress := range ingresses {
			ingress.ObjectMeta.OwnerReferences = []metav1.OwnerReference{CreateOwnerReference(submittedPod)}
			_, err = allocationService.clusterContext.SubmitIngress(ingress)
			if err != nil {
				return pod, err
			}
		}
		return pod, err
	} else {
		_, err := allocationService.clusterContext.SubmitPod(pod, job.Owner, job.QueueOwnershipUserGroups)
		return pod, err
	}
}

func exposesPorts(job *api.Job, podSpec *v1.PodSpec) bool {
	return len(GetServicePorts(job.Ingress, podSpec)) > 0
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
