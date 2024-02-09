package service

import (
	"fmt"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/armadaproject/armada/internal/common/healthmonitor"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	util2 "github.com/armadaproject/armada/internal/common/util"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/pkg/api"
)

type ClusterAllocator interface {
	AllocateSpareClusterCapacity()
}

type ClusterAllocationService struct {
	clusterId            executorContext.ClusterIdentity
	jobRunStateStore     job.RunStateStore
	submitter            job.Submitter
	eventReporter        reporter.EventReporter
	clusterHealthMonitor healthmonitor.HealthMonitor
}

func NewClusterAllocationService(
	clusterId executorContext.ClusterIdentity,
	eventReporter reporter.EventReporter,
	jobRunStateManager job.RunStateStore,
	submitter job.Submitter,
	clusterHealthMonitor healthmonitor.HealthMonitor,
) *ClusterAllocationService {
	return &ClusterAllocationService{
		eventReporter:        eventReporter,
		clusterId:            clusterId,
		submitter:            submitter,
		jobRunStateStore:     jobRunStateManager,
		clusterHealthMonitor: clusterHealthMonitor,
	}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	// If a health monitor is provided, avoid leasing jobs when the cluster is unhealthy.
	if allocationService.clusterHealthMonitor != nil {
		log := logrus.NewEntry(logrus.New())
		if ok, reason, err := allocationService.clusterHealthMonitor.IsHealthy(); err != nil {
			logging.WithStacktrace(log, err).Error("failed to check cluster health")
			return
		} else if !ok {
			log.Warnf("cluster is not healthy; will not request more jobs: %s", reason)
			return
		}
	}

	jobRuns := allocationService.jobRunStateStore.GetAllWithFilter(func(state *job.RunState) bool {
		return state.Phase == job.Leased
	})
	jobs := make([]*job.SubmitJob, 0, len(jobRuns))
	for _, run := range jobRuns {
		if run.Job == nil {
			// TODO report invalid - when we drive events off state
			log.Errorf("Job for job %s run %s unexpectedly nil", run.Meta.JobId, run.Meta.RunId)
			continue
		}
		jobs = append(jobs, run.Job)
	}

	failedJobSubmissions := allocationService.submitter.SubmitJobs(jobs)
	allocationService.processSuccessfulSubmissions(jobs, failedJobSubmissions)
	allocationService.processFailedJobSubmissions(failedJobSubmissions)
}

func (allocationService *ClusterAllocationService) processSuccessfulSubmissions(jobs []*job.SubmitJob, failedSubmissions []*job.FailedSubmissionDetails) {
	failedSubmissionSet := make(map[string]bool, len(failedSubmissions))
	for _, failedSubmission := range failedSubmissions {
		failedSubmissionSet[failedSubmission.JobRunMeta.RunId] = true
	}
	for _, j := range jobs {
		if _, failed := failedSubmissionSet[j.Meta.RunMeta.RunId]; !failed {
			allocationService.jobRunStateStore.ReportSuccessfulSubmission(j.Meta.RunMeta.RunId)
		}
	}
}

func (allocationService *ClusterAllocationService) processFailedJobSubmissions(failedSubmissions []*job.FailedSubmissionDetails) {
	for _, details := range failedSubmissions {
		message := details.Error.Error()
		if apiError, ok := details.Error.(errors.APIStatus); ok {
			message = apiError.Status().Message
		}

		if details.Recoverable {
			returnLeaseEvent := reporter.CreateReturnLeaseEvent(details.Pod, message, allocationService.clusterId.GetClusterId(), true)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: details.JobRunMeta.RunId}})
			if err == nil {
				allocationService.jobRunStateStore.ReportFailedSubmission(details.JobRunMeta.RunId)
			} else {
				// This will cause us to lease it again - which is acceptable as the pod was never created
				// Longer term we'll just update the state and let the state manager handle resending this event
				allocationService.jobRunStateStore.Delete(details.JobRunMeta.RunId)
				log.Errorf("Failed to return lease for job %s because %s", details.JobRunMeta.JobId, err)
			}
		} else {
			failEvent := reporter.CreateSimpleJobFailedEvent(details.Pod, message, allocationService.clusterId.GetClusterId(), api.Cause_Error)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: failEvent, JobRunId: details.JobRunMeta.RunId}})
			if err == nil {
				allocationService.jobRunStateStore.ReportFailedSubmission(details.JobRunMeta.RunId)
			} else {
				// This will cause us to lease it again - which is acceptable as the pod was never created
				// Longer term we'll just update the state and let the state manager handle resending this event
				allocationService.jobRunStateStore.Delete(details.JobRunMeta.RunId)
				log.Errorf("Failed to report submission as failed for job %s because %s", details.JobRunMeta.JobId, err)
			}
		}
	}
}

func formatResources(availableResource armadaresource.ComputeResources) string {
	cpu := availableResource["cpu"]
	memory := availableResource["memory"]
	ephemeralStorage := availableResource["ephemeral-storage"]

	resources := fmt.Sprintf(
		"cpu: %dm, memory %s, ephemeral-storage: %s",
		cpu.MilliValue(), util2.FormatBinarySI(memory.Value()), util2.FormatBinarySI(ephemeralStorage.Value()),
	)

	nvidiaGpu := availableResource["nvidia.com/gpu"]
	if nvidiaGpu.Value() > 0 {
		resources += fmt.Sprintf(", nvidia.com/gpu: %d", nvidiaGpu.Value())
	}
	amdGpu := availableResource["amd.com/gpu"]
	if amdGpu.Value() > 0 {
		resources += fmt.Sprintf(", amd.com/gpu: %d", nvidiaGpu.Value())
	}
	return resources
}
