package job

import (
	context2 "context"
	"fmt"
	"github.com/armadaproject/armada/internal/common/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
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
)

type Submitter interface {
	SubmitApiJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails
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
	JobId       string
	Pod         *v1.Pod
	Error       error
	Recoverable bool
}

func (submitService *SubmitService) SubmitApiJobs(jobsToSubmit []*api.Job) []*FailedSubmissionDetails {
	submitJobs := CreateSubmitJobsFromApiJobs(jobsToSubmit, submitService.podDefaults)
	return submitService.submitJobs(submitJobs)
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
		var jobPods []*v1.Pod
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
		}
	}
}

// submitPod submits a pod to k8s together with any services and ingresses bundled with the Armada job.
// This function may fail partly, i.e., it may successfully create a subset of the requested objects before failing.
// In case of failure, any already created objects are not cleaned up.
func (submitService *SubmitService) submitPod(job *SubmitJob) (*v1.Pod, error) {
	if carrier := job.Meta.JobMeta["carrier"]; carrier != nil {
		ctx, _ := tracing.ExtractTraceCtx(context2.Background(), carrier)
		tp := otel.Tracer("executor")
		ctx, span := tp.Start(ctx, "SubmitService.submitPod")
		defer span.End()
		bg := baggage.FromContext(ctx)
		span.SetAttributes(
			attribute.String("queue", bg.Member("queue").Value()),
			attribute.String("jobSetId", bg.Member("jobSetId").Value()),
			attribute.String("userId", bg.Member("userId").Value()),
		)

		traceparent := tracing.ExtractTraceparent(ctx)
		traceID := span.SpanContext().TraceID()
		spanID := span.SpanContext().SpanID()
		otelEnv := []v1.EnvVar{
			{Name: "OTEL_TRACEPARENT", Value: traceparent},
			{Name: "OTEL_TRACE_ID", Value: traceID.String()},
			{Name: "OTEL_SPAN_ID", Value: spanID.String()},
			{Name: "OTEL_WORKLOAD_NAME", Value: job.Pod.Name},
		}
		job.Pod.Spec.Containers[0].Env = append(job.Pod.Spec.Containers[0].Env, otelEnv...)
	}

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

// applyExecutorSpecificIngressDetails populates the executor specific details on ingresses
// These objects are mostly created server side however there will be details that are not known until submit time
// So the executor must fill them in before it creates the objects in kubernetes
func (submitService *SubmitService) applyExecutorSpecificIngressDetails(job *SubmitJob) {
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
