package service

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	util2 "github.com/armadaproject/armada/internal/common/util"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
)

type ClusterAllocator interface {
	AllocateSpareClusterCapacity()
}

type ClusterAllocationService struct {
	clusterId         executorContext.ClusterIdentity
	jobRunStateStore  job.RunStateStore
	submitter         job.Submitter
	eventReporter     reporter.EventReporter
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor
}

func NewClusterAllocationService(
	clusterId executorContext.ClusterIdentity,
	eventReporter reporter.EventReporter,
	jobRunStateManager job.RunStateStore,
	submitter job.Submitter,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
) *ClusterAllocationService {
	return &ClusterAllocationService{
		eventReporter:     eventReporter,
		clusterId:         clusterId,
		submitter:         submitter,
		jobRunStateStore:  jobRunStateManager,
		etcdHealthMonitor: etcdHealthMonitor,
	}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	// If a health monitor is provided, avoid leasing jobs when etcd is almost full.
	if allocationService.etcdHealthMonitor != nil && !allocationService.etcdHealthMonitor.IsWithinSoftHealthLimit() {
		log.Warnf("Skipping allocating spare cluster capacity as etcd is at its soft health limit")
		return
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

type LegacyClusterAllocationService struct {
	leaseService       LeaseService
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     executorContext.ClusterContext
	submitter          job.Submitter
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
}

func NewLegacyClusterAllocationService(
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseService LeaseService,
	utilisationService utilisation.UtilisationService,
	submitter job.Submitter,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
) *LegacyClusterAllocationService {
	return &LegacyClusterAllocationService{
		leaseService:       leaseService,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext,
		submitter:          submitter,
		etcdHealthMonitor:  etcdHealthMonitor,
	}
}

func (allocationService *LegacyClusterAllocationService) AllocateSpareClusterCapacity() {
	// If a health monitor is provided, avoid leasing jobs when etcd is almost full.
	if allocationService.etcdHealthMonitor != nil && !allocationService.etcdHealthMonitor.IsWithinSoftHealthLimit() {
		log.Warnf("Skipping allocating spare cluster capacity as etcd is at its soft health limit")
		return
	}

	capacityReport, err := allocationService.utilisationService.GetAvailableClusterCapacity(true)
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	leasePods, err := allocationService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}
	activePods := util.FilterPods(leasePods, isActive)
	newJobs, err := allocationService.leaseService.RequestJobLeases(
		capacityReport.AvailableCapacity,
		capacityReport.Nodes,
		utilisation.GetAllocationByQueue(activePods),
		utilisation.GetAllocationByQueueAndPriority(activePods),
	)
	logAvailableResources(*capacityReport.AvailableCapacity, len(newJobs))
	if err != nil {
		log.Errorf("failed to lease new jobs: %v", err)
		return
	}

	failedJobs := allocationService.submitter.SubmitApiJobs(newJobs)
	if err := allocationService.processFailedJobs(failedJobs); err != nil {
		log.Errorf("failed to process failed jobs: %v", err)
	}
}

func logAvailableResources(availableResource armadaresource.ComputeResources, jobCount int) {
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

	log.Infof("Reporting current free resource %s. Received %d new jobs. ", resources, jobCount)
}

// Any pod not in a terminal state is considered active for the purposes of cluster allocation
// As soon as a pod finishes (enters a terminal state) we should try to allocate another pod
func isActive(pod *v1.Pod) bool {
	return !util.IsInTerminalState(pod)
}

func (allocationService *LegacyClusterAllocationService) processFailedJobs(failedSubmissions []*job.FailedSubmissionDetails) error {
	toBeReportedDone := make([]string, 0, 10)

	for _, details := range failedSubmissions {
		message := details.Error.Error()
		if apiError, ok := details.Error.(errors.APIStatus); ok {
			message = apiError.Status().Message
		}

		if details.Recoverable {
			allocationService.returnLease(details.Pod, fmt.Sprintf("Failed to submit pod because %s", message))
		} else {
			failEvent := reporter.CreateSimpleJobFailedEvent(details.Pod, message, allocationService.clusterContext.GetClusterId(), api.Cause_Error)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: failEvent, JobRunId: util.ExtractJobRunId(details.Pod)}})

			if err == nil {
				toBeReportedDone = append(toBeReportedDone, details.JobRunMeta.JobId)
			}
		}
	}

	return allocationService.leaseService.ReportDone(toBeReportedDone)
}

func (allocationService *LegacyClusterAllocationService) returnLease(pod *v1.Pod, reason string) {
	err := allocationService.leaseService.ReturnLease(pod, reason, true)
	if err != nil {
		log.Errorf("Failed to return lease for job %s because %s", util.ExtractJobId(pod), err)
	}
}
