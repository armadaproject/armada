package job

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	util2 "github.com/armadaproject/armada/internal/executor/util"
)

// When a job retries, the previous pod's deletion triggers async garbage collection
// of owned resources (services/ingresses via OwnerReference). Creating new resources
// may fail with AlreadyExists if GC hasn't completed. We use delete-then-create
// with retries to handle this race condition.
// Retry delays use linear backoff: 100ms, 200ms, 300ms, 400ms, 500ms.
const (
	serviceIngressMaxRetries     = 5
	serviceIngressBaseRetryDelay = 100 * time.Millisecond
)

type Submitter interface {
	SubmitJobs(jobsToSubmit []*SubmitJob) []*FailedSubmissionDetails
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
	JobRunMeta  *RunMeta
	Pod         *v1.Pod
	Error       error
	Recoverable bool
}

func (submitService *SubmitService) SubmitJobs(jobsToSubmit []*SubmitJob) []*FailedSubmissionDetails {
	return submitService.submitJobs(jobsToSubmit)
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
			log.Errorf("Failed to submit job %s because %s", job.Meta.RunMeta.JobId, err)

			errDetails := &FailedSubmissionDetails{
				JobRunMeta:  job.Meta.RunMeta,
				Pod:         pod,
				Error:       err,
				Recoverable: submitService.isRecoverable(err),
			}

			failedJobsChannel <- errDetails

			// remove just created pods
			submitService.clusterContext.DeletePods(jobPods)
		}
	}
}

// submitPod submits a pod to k8s together with any services and ingresses bundled with the Armada job.
// This function may fail partly, i.e., it may successfully create a subset of the requested objects before failing.
// In case of failure, any already created objects are not cleaned up.
func (submitService *SubmitService) submitPod(job *SubmitJob) (*v1.Pod, error) {
	pod := job.Pod
	// Ensure the K8SService and K8SIngress fields are populated
	submitService.applyExecutorSpecificIngressDetails(job)

	if len(job.Ingresses) > 0 || len(job.Services) > 0 {
		pod.Annotations = util.MergeMaps(pod.Annotations, map[string]string{
			domain.HasIngress:               "true",
			domain.AssociatedServicesCount:  fmt.Sprintf("%d", len(job.Services)),
			domain.AssociatedIngressesCount: fmt.Sprintf("%d", len(job.Ingresses)),
		})
	}

	submittedPod, err := submitService.clusterContext.SubmitPod(pod, job.Meta.Owner, job.Meta.OwnershipGroups)
	if err != nil {
		return pod, err
	}

	for _, service := range job.Services {
		service.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		err = submitService.createServiceWithRetry(service)
		if err != nil {
			return pod, err
		}
	}

	for _, ingress := range job.Ingresses {
		ingress.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		err = submitService.createIngressWithRetry(ingress)
		if err != nil {
			return pod, err
		}
	}

	return pod, err
}

// applyExecutorSpecificIngressDetails populates the executor specific details on ingresses
// These objects are mostly created server side however there will be details that are not known until submit time
// So the executor must fill them in before it creates the objects in kubernetes
func (submitService *SubmitService) applyExecutorSpecificIngressDetails(job *SubmitJob) {
	if submitService.podDefaults == nil || submitService.podDefaults.Ingress == nil {
		return
	}
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

// createServiceWithRetry creates a service with retry handling for GC race conditions.
func (submitService *SubmitService) createServiceWithRetry(service *v1.Service) error {
	return submitService.createWithRetry(
		"service",
		service.Namespace,
		service.Name,
		func() error { return submitService.clusterContext.DeleteService(service) },
		func() error { _, err := submitService.clusterContext.SubmitService(service); return err },
	)
}

// createIngressWithRetry creates an ingress with retry handling for GC race conditions.
func (submitService *SubmitService) createIngressWithRetry(ingress *networking.Ingress) error {
	return submitService.createWithRetry(
		"ingress",
		ingress.Namespace,
		ingress.Name,
		func() error { return submitService.clusterContext.DeleteIngress(ingress) },
		func() error { _, err := submitService.clusterContext.SubmitIngress(ingress); return err },
	)
}

// createWithRetry implements a delete-then-create pattern with retries to handle
// race conditions when garbage collection is still running from a previous run.
func (submitService *SubmitService) createWithRetry(
	resourceType string,
	namespace string,
	name string,
	deleteFn func() error,
	createFn func() error,
) error {
	for attempt := 0; attempt < serviceIngressMaxRetries; attempt++ {
		if err := deleteFn(); err != nil {
			return errors.Wrapf(err, "failed to delete existing %s %s/%s", resourceType, namespace, name)
		}

		err := createFn()
		if err == nil {
			return nil
		}

		if !k8s_errors.IsAlreadyExists(err) {
			return err
		}

		if attempt < serviceIngressMaxRetries-1 {
			retryDelay := time.Duration(attempt+1) * serviceIngressBaseRetryDelay
			log.Warnf("%s %s/%s already exists after delete (attempt %d/%d), retrying after %v",
				resourceType, namespace, name, attempt+1, serviceIngressMaxRetries, retryDelay)
			time.Sleep(retryDelay)
		}
	}

	return errors.Errorf("failed to create %s %s/%s: still exists after %d delete attempts",
		resourceType, namespace, name, serviceIngressMaxRetries)
}
