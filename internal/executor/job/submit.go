package job

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	util2 "github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type Submitter interface {
	SubmitApiJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails
	SubmitExecutorApiJobs(jobsToSubmit []*executorapi.JobRunLease) []*FailedSubmissionDetails
}

type SubmitService struct {
	clusterContext           context.ClusterContext
	podDefaults              *configuration.PodDefaults
	submissionThreadCount    int
	fatalPodSubmissionErrors []string
}

func NewSubmitter(
	clusterContext context.ClusterContext,
	podDefaults *configuration.PodDefaults,
	submissionThreadCount int,
	fatalPodSubmissionErrors []string,
) *SubmitService {
	return &SubmitService{
		clusterContext:           clusterContext,
		podDefaults:              podDefaults,
		submissionThreadCount:    submissionThreadCount,
		fatalPodSubmissionErrors: fatalPodSubmissionErrors,
	}
}

type FailedSubmissionDetails struct {
	JobId       string
	Pod         *v1.Pod
	Error       error
	Recoverable bool
}

func (submitService *SubmitService) SubmitApiJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails {
	submitJobs := CreateSubmitJobsFromApiJobs(jobsToSubmit, submitService.podDefaults)
	return submitService.submitJobs(submitJobs)
}

func (submitService *SubmitService) SubmitExecutorApiJobs(jobsToSubmit []*executorapi.JobRunLease) []*FailedSubmissionDetails {
	submitJobs := make([]*SubmitJob, 0, len(jobsToSubmit))
	failedSubmissions := []*FailedSubmissionDetails{}
	for _, jobToSubmit := range jobsToSubmit {
		submitJob, err := CreateSubmitJobFromExecutorApiJobRunLease(jobToSubmit, submitService.podDefaults)
		if err != nil {
			failedSubmissions = append(failedSubmissions, &FailedSubmissionDetails{
				JobId: jobToSubmit.Job.JobId.String(),
				//TODO work out how to handle that we have no pod - especially in downstream funcs
				Pod:         nil,
				Recoverable: false,
				Error:       err,
			})
		} else {
			submitJobs = append(submitJobs, submitJob)
		}
	}
	failedSubmissions = append(failedSubmissions, submitService.submitJobs(submitJobs)...)
	return failedSubmissions
}

func (submitService *SubmitService) submitJobs(jobsToSubmit []*SubmitJob) []*FailedSubmissionDetails {
	wg := &sync.WaitGroup{}
	submitJobsChannel := make(chan *SubmitJob)
	failedJobsChannel := make(chan *FailedSubmissionDetails, len(jobsToSubmit))

	for i := 0; i < submitService.submissionThreadCount; i++ {
		wg.Add(1)
		go submitService.submitWorker(wg, submitJobsChannel, failedJobsChannel)
	}

	for _, job := range jobsToSubmit {
		submitJobsChannel <- job
	}

	close(submitJobsChannel)
	wg.Wait()
	close(failedJobsChannel)

	toBeFailedJobs := make([]*FailedSubmissionDetails, 0, len(failedJobsChannel))
	for failedJob := range failedJobsChannel {
		toBeFailedJobs = append(toBeFailedJobs, failedJob)
	}

	return toBeFailedJobs
}

func (submitService *SubmitService) submitWorker(wg *sync.WaitGroup, jobsToSubmitChannel chan *SubmitJob, failedJobsChannel chan *FailedSubmissionDetails) {
	defer wg.Done()

	for job := range jobsToSubmitChannel {
		jobPods := []*v1.Pod{}
		pod, err := submitService.submitPod(job)
		jobPods = append(jobPods, pod)

		if err != nil {
			log.Errorf("Failed to submit job %s because %s", job.Meta.JobId, err)

			errDetails := &FailedSubmissionDetails{
				JobId:       job.Meta.JobId,
				Pod:         pod,
				Error:       err,
				Recoverable: submitService.isRecoverable(err),
			}

			failedJobsChannel <- errDetails

			// remove just created pods
			submitService.clusterContext.DeletePods(jobPods)
			break
		}
	}
}

// submitPod submits a pod to k8s together with any services and ingresses bundled with the Armada job.
// This function may fail partly, i.e., it may successfully create a subset of the requested objects before failing.
// In case of failure, any already created objects are not cleaned up.
func (submitService *SubmitService) submitPod(job *SubmitJob) (*v1.Pod, error) {
	pod := job.Pod
	// Ensure the K8SService and K8SIngress fields are populated
	submitService.applyExecutorSpecificIngressDetails(job, pod)

	if len(job.Ingresses) > 0 || len(job.Services) > 0 {
		pod.Annotations = util.MergeMaps(pod.Annotations, map[string]string{
			domain.HasIngress:               "true",
			domain.AssociatedServicesCount:  fmt.Sprintf("%d", len(job.Ingresses)),
			domain.AssociatedIngressesCount: fmt.Sprintf("%d", len(job.Services)),
		})
	}

	submittedPod, err := submitService.clusterContext.SubmitPod(pod, job.Meta.Owner, job.Meta.OwnershipGroups)
	if err != nil {
		return pod, err
	}

	for _, service := range job.Services {
		service.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		_, err = submitService.clusterContext.SubmitService(service)
		if err != nil {
			return pod, err
		}
	}

	for _, ingress := range job.Ingresses {
		ingress.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		_, err = submitService.clusterContext.SubmitIngress(ingress)
		if err != nil {
			return pod, err
		}
	}

	return pod, err
}

// applyExecutorSpecificIngressDetails populates the K8SService and K8SIngress fields of the job.
// It does so by converting the Services and Ingress fields, which are Armada-specific, into proper k8s objects.
// If either of K8SService or K8SIngress is already populated (i.e., is non-nil), this function is a no-op,
// except for replacing nil-valued K8SService and K8SIngress with empty slices.
//
// TODO: I think this is wrong for jobs with multiple PodSPecs.
// Because we should create a set of services/ingresses for each pod,
// but this code is a no-op if K8SService or K8SIngress is already populated.
func (submitService *SubmitService) applyExecutorSpecificIngressDetails(job *SubmitJob, pod *v1.Pod) {
	// If K8SIngress and/or K8SService was already populated, it was populated by the Pulsar submit API.
	// Because the submit API can't know which executor the services/ingresses will be created in,
	// the executor has to provide all executor-specific information.
	for _, ingress := range job.Ingresses {
		ingress.Annotations = util.MergeMaps(
			ingress.Annotations,
			submitService.podDefaults.Ingress.Annotations,
		)

		// We need to use indexing here since Spec.Rules isn't pointers.
		for i := range ingress.Spec.Rules {
			ingress.Spec.Rules[i].Host += submitService.podDefaults.Ingress.HostnameSuffix
		}

		// We need to use indexing here since Spec.TLS isn't pointers.
		for i := range ingress.Spec.TLS {
			ingress.Spec.TLS[i].SecretName += submitService.podDefaults.Ingress.CertNameSuffix
			for j := range ingress.Spec.TLS[i].Hosts {
				ingress.Spec.TLS[i].Hosts[j] += submitService.podDefaults.Ingress.HostnameSuffix
			}
		}
	}
}

func exposesPorts(job *api.Job, podSpec *v1.PodSpec) bool {
	// This is to workaround needing to get serviceports for service configs
	// while maintaining immutability of the configs as they're passed around.
	servicesIngressConfig := util2.CombineIngressService(job.Ingress, job.Services)

	return len(util2.GetServicePorts(servicesIngressConfig, podSpec)) > 0
}

func (submitService *SubmitService) isRecoverable(err error) bool {
	if apiStatus, ok := err.(k8s_errors.APIStatus); ok {
		status := apiStatus.Status()
		if status.Reason == metav1.StatusReasonInvalid ||
			status.Reason == metav1.StatusReasonForbidden {
			return false
		}

		for _, errorMessage := range submitService.fatalPodSubmissionErrors {
			ok, err := regexp.MatchString(errorMessage, err.Error())
			if err == nil && ok {
				return false
			}
		}

		return true
	}

	var e *armadaerrors.ErrCreateResource
	if errors.As(err, &e) {
		return true
	}

	return false
}
