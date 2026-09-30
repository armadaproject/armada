package job

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
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

type Submitter interface {
	SubmitJobs(jobsToSubmit []*SubmitJob) []*FailedSubmissionDetails
}

type SubmitService struct {
	clusterContext           context.ClusterContext
	podDefaults              *configuration.PodDefaults
	submissionThreadCount    int
	fatalPodSubmissionErrors []string
	runScopedPodNames        bool
}

func NewSubmitter(
	clusterContext context.ClusterContext,
	podDefaults *configuration.PodDefaults,
	submissionThreadCount int,
	fatalPodSubmissionErrors []string,
	runScopedPodNames bool,
) *SubmitService {
	return &SubmitService{
		clusterContext:           clusterContext,
		podDefaults:              podDefaults,
		submissionThreadCount:    submissionThreadCount,
		fatalPodSubmissionErrors: fatalPodSubmissionErrors,
		runScopedPodNames:        runScopedPodNames,
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
		pod, podOfOtherRun, err := submitService.submitPod(job)
		if err != nil {
			log.Errorf("Failed to submit job %s because %s", job.Meta.RunMeta.JobId, err)

			errDetails := &FailedSubmissionDetails{
				JobRunMeta:  job.Meta.RunMeta,
				Pod:         pod,
				Error:       err,
				Recoverable: submitService.isRecoverable(err),
			}

			failedJobsChannel <- errDetails

			// The delete by name frees a job-scoped name that a pod of an earlier run holds. It also deletes the pod
			// of this run when a service or an ingress create fails. A pod of another run with a run-scoped name stays.
			if !podOfOtherRun {
				submitService.clusterContext.DeletePods([]*v1.Pod{pod})
			}
		}
	}
}

// submitPod submits a pod to k8s together with any services and ingresses bundled with the Armada job.
// This function may fail partly, i.e., it may successfully create a subset of the requested objects before failing.
// In case of failure, any already created objects are not cleaned up.
// The returned bool is true when the pod that holds the run-scoped name belongs to another run, or to an unknown run.
// The caller then keeps that pod.
func (submitService *SubmitService) submitPod(job *SubmitJob) (*v1.Pod, bool, error) {
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
	if submitService.isRunScopedConflict(err) {
		var podOfOtherRun bool
		submittedPod, podOfOtherRun, err = submitService.existingPodOfRun(pod, err)
		if podOfOtherRun {
			return pod, true, err
		}
	}
	if err != nil {
		return pod, false, err
	}

	for _, service := range job.Services {
		service.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		_, err = submitService.clusterContext.SubmitService(service)
		if submitService.isRunScopedConflict(err) {
			existing, getErr := submitService.clusterContext.GetService(service.Namespace, service.Name)
			if getErr == nil {
				err = errUnlessObjectOfRun(&existing.ObjectMeta, pod, err)
			}
		}
		if err != nil {
			return pod, false, err
		}
	}

	for _, ingress := range job.Ingresses {
		ingress.ObjectMeta.OwnerReferences = []metav1.OwnerReference{util2.CreateOwnerReference(submittedPod)}
		_, err = submitService.clusterContext.SubmitIngress(ingress)
		if submitService.isRunScopedConflict(err) {
			existing, getErr := submitService.clusterContext.GetIngress(ingress.Namespace, ingress.Name)
			if getErr == nil {
				err = errUnlessObjectOfRun(&existing.ObjectMeta, pod, err)
			}
		}
		if err != nil {
			return pod, false, err
		}
	}

	return pod, false, nil
}

// isRunScopedConflict is true when a create fails because the object exists and pod names are run-scoped.
func (submitService *SubmitService) isRunScopedConflict(err error) bool {
	return err != nil && submitService.runScopedPodNames && k8s_errors.IsAlreadyExists(err)
}

// errUnlessObjectOfRun returns nil when an existing service or ingress belongs to the run of the pod, for example after a
// duplicate lease, and Kubernetes is not deleting it. Services and ingresses keep one name per job, so the object can
// also belong to an earlier run, until the cleanup of that run removes it. An object that Kubernetes is deleting goes
// away after the run starts. In both cases it returns the original error, which is recoverable, so the lease goes back
// and a later run creates the object.
func errUnlessObjectOfRun(existing metav1.Object, pod *v1.Pod, alreadyExistsErr error) error {
	if existing.GetDeletionTimestamp() == nil && existing.GetLabels()[domain.JobRunId] == util2.ExtractJobRunId(pod) {
		return nil
	}
	return alreadyExistsErr
}

// existingPodOfRun reads the pod that holds a run-scoped name. A pod of the same run, for example after a duplicate
// lease, is a success, unless Kubernetes is deleting it. For a pod of another run or a pod that Kubernetes is deleting,
// it returns the original error and true, so that pod stays and the lease goes back. When the read fails, the run is
// unknown, so it also returns true. It returns the original error in that case too, because isRecoverable treats an
// error that is not an API status as permanent.
func (submitService *SubmitService) existingPodOfRun(pod *v1.Pod, alreadyExistsErr error) (*v1.Pod, bool, error) {
	existing, err := submitService.clusterContext.GetPod(pod.Namespace, pod.Name)
	if err != nil {
		log.Warnf("Failed to read pod %s (%s) that holds the name of run %s: %v", pod.Name, pod.Namespace, util2.ExtractJobRunId(pod), err)
		return nil, true, alreadyExistsErr
	}
	if existing.DeletionTimestamp != nil || util2.ExtractJobRunId(existing) != util2.ExtractJobRunId(pod) {
		return nil, true, alreadyExistsErr
	}
	return existing, false, nil
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
