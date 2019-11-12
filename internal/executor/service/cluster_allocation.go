package service

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

const PodNamePrefix string = "batch-"

type ClusterAllocationService struct {
	leaseService       LeaseService
	eventReporter      reporter.EventReporter
	utilisationService UtilisationService
	clusterContext     context.ClusterContext
}

func NewClusterAllocationService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseService LeaseService,
	utilisationService UtilisationService) *ClusterAllocationService {

	return &ClusterAllocationService{
		leaseService:       leaseService,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	availableResource, err := allocationService.utilisationService.GetAvailableClusterCapacity()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	newJobs, err := allocationService.leaseService.RequestJobLeases(availableResource)

	cpu := (*availableResource)["cpu"]
	memory := (*availableResource)["memory"]
	log.Infof("Requesting new jobs with free resource cpu: %d, memory %d. Received %d new jobs. ", cpu.AsDec(), memory.Value(), len(newJobs))

	if err != nil {
		log.Errorf("Failed to lease new jobs because %s", err)
		return
	} else {
		allocationService.submitJobs(newJobs)
	}
}

func (allocationService *ClusterAllocationService) submitJobs(jobsToSubmit []*api.Job) {
	toBeFailedJobs := make([]*failedSubmissionDetails, 0, 10)

	for _, job := range jobsToSubmit {
		pod := createPod(job)
		_, err := allocationService.clusterContext.SubmitPod(pod, job.Owner)

		if err != nil {
			log.Errorf("Failed to submit job %s because %s", job.Id, err)

			status, ok := err.(errors.APIStatus)
			if ok && (isNotRecoverable(status.Status().Reason)) {
				errDetails := &failedSubmissionDetails{
					pod:   pod,
					error: status,
				}
				toBeFailedJobs = append(toBeFailedJobs, errDetails)
			} else {
				allocationService.returnLease(pod, fmt.Sprintf("Failed to submit pod because %s", err))
			}
		}
	}

	err := allocationService.failJobs(toBeFailedJobs)
	if err != nil {
		log.Errorf("Failed to report failed jobs as done because %s", err)
	}
}

func isNotRecoverable(reason metav1.StatusReason) bool {
	return reason == metav1.StatusReasonInvalid ||
		reason == metav1.StatusReasonForbidden
}

func (allocationService *ClusterAllocationService) failJobs(failedSubmissions []*failedSubmissionDetails) error {
	toBeReportedDone := make([]*v1.Pod, 0, 10)

	for _, details := range failedSubmissions {
		failEvent := reporter.CreateJobFailedEvent(details.pod, details.error.Status().Message, allocationService.clusterContext.GetClusterId())
		err := allocationService.eventReporter.Report(failEvent)

		if err == nil {
			toBeReportedDone = append(toBeReportedDone, details.pod)
		}
	}

	return allocationService.leaseService.ReportDone(toBeReportedDone)
}

func (allocationService *ClusterAllocationService) returnLease(pod *v1.Pod, reason string) {
	err := allocationService.leaseService.ReturnLease(pod)

	if err != nil {
		log.Errorf("Failed to return lease for job %s because %s", util.ExtractJobId(pod), err)
	} else {
		leaseReturnedEvent := reporter.CreateJobLeaseReturnedEvent(pod, reason, allocationService.clusterContext.GetClusterId())

		err = allocationService.eventReporter.Report(leaseReturnedEvent)
		if err != nil {
			log.Errorf("Failed to report event %+v because %s", leaseReturnedEvent, err)
		}
	}
}

func createPod(job *api.Job) *v1.Pod {
	labels := createLabels(job)
	setRestartPolicyNever(job.PodSpec)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PodNamePrefix + job.Id,
			Labels:    labels,
			Namespace: job.Namespace,
		},
		Spec: *job.PodSpec,
	}

	return &pod
}

func setRestartPolicyNever(podSpec *v1.PodSpec) {
	podSpec.RestartPolicy = v1.RestartPolicyNever
}

func createLabels(job *api.Job) map[string]string {
	labels := make(map[string]string)

	labels[domain.JobId] = job.Id
	labels[domain.JobSetId] = job.JobSetId
	labels[domain.Queue] = job.Queue

	return labels
}

type failedSubmissionDetails struct {
	pod   *v1.Pod
	error errors.APIStatus
}
