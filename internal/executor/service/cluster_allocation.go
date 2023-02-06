package service

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/executorapi"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type ClusterAllocator interface {
	AllocateSpareClusterCapacity()
}

type ClusterAllocationService struct {
	leaseRequester     LeaseRequester
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     context.ClusterContext
	submitter          job.Submitter
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
}

func NewClusterAllocationService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseRequester LeaseRequester,
	utilisationService utilisation.UtilisationService,
	submitter job.Submitter,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
) *ClusterAllocationService {
	return &ClusterAllocationService{
		leaseRequester:     leaseRequester,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext,
		submitter:          submitter,
		etcdHealthMonitor:  etcdHealthMonitor,
	}
}

func (allocationService *ClusterAllocationService) AllocateSpareClusterCapacity() {
	// If a health monitor is provided, avoid leasing jobs when etcd is almost full.
	if allocationService.etcdHealthMonitor != nil && !allocationService.etcdHealthMonitor.IsWithinSoftHealthLimit() {
		log.Warnf("Skipping allocating spare cluster capacity as etcd is at its soft health limit")
		return
	}

	capacityReport, err := allocationService.utilisationService.GetAvailableClusterCapacity(false)
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	unassignedRunIds, err := allocationService.getUnassignedRunIds()
	if err != nil {
		log.Errorf("Failed to allocate spare cluster capacity because %s", err)
		return
	}

	nodes := make([]*api.NodeInfo, 0, len(capacityReport.Nodes))
	for i := range capacityReport.Nodes {
		nodes = append(nodes, &capacityReport.Nodes[i])
	}

	newJobRuns, runsToCancel, err := allocationService.leaseRequester.LeaseJobRuns(
		capacityReport.AvailableCapacity,
		nodes,
		unassignedRunIds,
	)
	if err != nil {
		log.Errorf("failed to lease new jobs: %v", err)
		return
	}

	logAvailableResources(capacityReport, len(newJobRuns))

	jobs, failedJobCreations := allocationService.createSubmitJobs()
	allocationService.handleFailedJobCreation(failedJobCreations)
	failedJobSubmissions := allocationService.submitter.SubmitJobs(jobs)
	allocationService.processFailedJobSubmissions(failedJobSubmissions)
	allocationService.processRunsToCancel(runsToCancel)
}

type failedJobCreationDetails struct {
	JobId  string
	RunId  string
	Queue  string
	JobSet string
	Error  error
}

func (allocationService *ClusterAllocationService) createSubmitJobs(newJobRuns []*executorapi.JobRunLease) ([]*job.SubmitJob, []*failedJobCreationDetails) {
	//submitJobs := make([]*job.SubmitJob, 0, len(newJobRuns))
	//failedCreation := []*failedJobCreationDetails{}
	//for _, jobToSubmit := range newJobRuns {
	//	submitJob, err := job.CreateSubmitJobFromExecutorApiJobRunLease(jobToSubmit, submitService.podDefaults)
	//	if err != nil {
	//		jobIdString, _ := armadaevents.UlidStringFromProtoUuid(jobToSubmit.Job.JobId)
	//	} else {
	//		submitJobs = append(submitJobs, submitJob)
	//	}
	//}
	//
	//return submitJobs,
}

func (allocationService *ClusterAllocationService) handleFailedJobCreation([]*failedJobCreationDetails) {

}

// Returns the RunIds of all managed pods that haven't been assigned to a node
func (allocationService *ClusterAllocationService) getUnassignedRunIds() ([]armadaevents.Uuid, error) {
	allManagedPods, err := allocationService.clusterContext.GetBatchPods()
	if err != nil {
		return nil, err
	}

	unassignedPods := util.FilterPods(allManagedPods, func(pod *v1.Pod) bool {
		return pod.Spec.NodeName == ""
	})

	runIds := make([]string, 0, len(unassignedPods))
	for _, unassignedPod := range unassignedPods {
		runId := util.ExtractJobRunId(unassignedPod)
		runIds = append(runIds, runId)
	}

	return util.StringUuidsToUuids(runIds)
}

func (allocationService *ClusterAllocationService) processFailedJobSubmissions(failedSubmissions []*job.FailedSubmissionDetails) {
	for _, details := range failedSubmissions {
		message := details.Error.Error()
		if apiError, ok := details.Error.(errors.APIStatus); ok {
			message = apiError.Status().Message
		}
		jobRunId := util.ExtractJobRunId(details.Pod)

		if details.Recoverable {
			returnLeaseEvent := reporter.CreateReturnLeaseEvent(details.Pod, message, allocationService.clusterContext.GetClusterId(), true)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: jobRunId}})
			if err != nil {
				log.Errorf("Failed to return lease for job %s because %s", details.JobId, err)
			}
		} else {
			failEvent := reporter.CreateSimpleJobFailedEvent(details.Pod, message, allocationService.clusterContext.GetClusterId(), api.Cause_Error)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: failEvent, JobRunId: jobRunId}})
			if err != nil {
				log.Errorf("Failed to report submission as failed for job %s because %s", details.JobId, err)
			}
		}
	}
}

func (allocationService *ClusterAllocationService) processRunsToCancel(runsToCancel []*armadaevents.Uuid) {
	runsToCancelStrings, err := util.UuidsToStrings(runsToCancel)
	if err != nil {
		log.Errorf("Failed to cancel runs because %s", err)
		return
	}

	runsToCancelSet := util2.StringListToSet(runsToCancelStrings)

	managedPods, err := allocationService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to cancel runs because unable to get a current managed pods due to %s", err)
		return
	}

	podsToDelete := make([]*v1.Pod, 0, len(runsToCancel))
	for _, pod := range managedPods {
		runId := util.ExtractJobRunId(pod)
		if _, ok := runsToCancelSet[runId]; ok {
			podsToDelete = append(podsToDelete, pod)
		}
	}
	allocationService.clusterContext.DeletePods(podsToDelete)
}

type LegacyClusterAllocationService struct {
	leaseService       LeaseService
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     context.ClusterContext
	submitter          job.Submitter
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
	reserved           armadaresource.ComputeResources
}

func NewLegacyClusterAllocationService(
	clusterContext context.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseService LeaseService,
	utilisationService utilisation.UtilisationService,
	submitter job.Submitter,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
	reserved armadaresource.ComputeResources,
) *LegacyClusterAllocationService {
	return &LegacyClusterAllocationService{
		leaseService:       leaseService,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext,
		submitter:          submitter,
		etcdHealthMonitor:  etcdHealthMonitor,
		reserved:           reserved,
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
	logAvailableResources(capacityReport, len(newJobs))
	if err != nil {
		log.Errorf("failed to lease new jobs: %v", err)
		return
	}

	failedJobs := allocationService.submitter.SubmitApiJobs(newJobs)
	if err := allocationService.processFailedJobs(failedJobs); err != nil {
		log.Errorf("failed to process failed jobs: %v", err)
	}
}

func logAvailableResources(capacityReport *utilisation.ClusterAvailableCapacityReport, jobCount int) {
	cpu := capacityReport.GetResourceQuantity("cpu")
	memory := capacityReport.GetResourceQuantity("memory")
	ephemeralStorage := capacityReport.GetResourceQuantity("ephemeral-storage")

	resources := fmt.Sprintf(
		"cpu: %dm, memory %s, ephemeral-storage: %s",
		cpu.MilliValue(), util2.FormatBinarySI(memory.Value()), util2.FormatBinarySI(ephemeralStorage.Value()),
	)

	nvidiaGpu := capacityReport.GetResourceQuantity("nvidia.com/gpu")
	if nvidiaGpu.Value() > 0 {
		resources += fmt.Sprintf(", nvidia.com/gpu: %d", nvidiaGpu.Value())
	}
	amdGpu := capacityReport.GetResourceQuantity("amd.com/gpu")
	if amdGpu.Value() > 0 {
		resources += fmt.Sprintf(", amd.com/gpu: %d", nvidiaGpu.Value())
	}

	log.Infof("Requesting new jobs with free resource %s. Received %d new jobs. ", resources, jobCount)
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
				toBeReportedDone = append(toBeReportedDone, details.JobId)
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
