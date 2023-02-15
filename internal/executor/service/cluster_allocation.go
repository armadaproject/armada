package service

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/slices"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type ClusterAllocator interface {
	AllocateSpareClusterCapacity()
}

type ClusterAllocationService struct {
	leaseRequester     LeaseRequester
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     executorContext.ClusterContext
	submitter          job.Submitter
	jobRunStateManager *job.JobRunStateManager
	podDefaults        *configuration.PodDefaults
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
}

func NewClusterAllocationService(
	clusterContext executorContext.ClusterContext,
	eventReporter reporter.EventReporter,
	leaseRequester LeaseRequester,
	jobRunStateManager *job.JobRunStateManager,
	utilisationService utilisation.UtilisationService,
	submitter job.Submitter,
	podDefaults *configuration.PodDefaults,
	etcdHealthMonitor healthmonitor.EtcdLimitHealthMonitor,
) *ClusterAllocationService {
	return &ClusterAllocationService{
		leaseRequester:     leaseRequester,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		clusterContext:     clusterContext,
		submitter:          submitter,
		jobRunStateManager: jobRunStateManager,
		podDefaults:        podDefaults,
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

	unassignedRunIds, err := allocationService.getUnassignedRunIds(capacityReport)
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

	jobs, failedJobCreations := allocationService.createSubmitJobs(newJobRuns)
	allocationService.markJobsLeased(jobs, failedJobCreations)
	allocationService.handleFailedJobCreation(failedJobCreations)
	failedJobSubmissions := allocationService.submitter.SubmitJobs(jobs)
	allocationService.processFailedJobSubmissions(failedJobSubmissions)
	allocationService.processRunsToCancel(runsToCancel)
}

func (allocationService *ClusterAllocationService) markJobsLeased(jobs []*job.SubmitJob, failedJobCreations []*failedJobCreationDetails) {
	for _, j := range jobs {
		allocationService.jobRunStateManager.ReportRunLeased(j.Meta.JobRunMeta)
	}

	for _, failedJob := range failedJobCreations {
		allocationService.jobRunStateManager.ReportRunLeased(failedJob.JobRunMeta)
	}
}

type failedJobCreationDetails struct {
	JobRunMeta *job.RunMetaInfo
	Error      error
}

func (allocationService *ClusterAllocationService) createSubmitJobs(newJobRuns []*executorapi.JobRunLease) ([]*job.SubmitJob, []*failedJobCreationDetails) {
	submitJobs := make([]*job.SubmitJob, 0, len(newJobRuns))
	failedJobCreations := []*failedJobCreationDetails{}
	for _, jobToSubmit := range newJobRuns {
		jobMeta, err := extractEssentialJobMetadata(jobToSubmit)
		if err != nil {
			log.Errorf("received invalid job - %s", err)
			continue
		}

		submitJob, err := job.CreateSubmitJobFromExecutorApiJobRunLease(jobToSubmit, allocationService.podDefaults)
		if err != nil {
			failedJobCreations = append(failedJobCreations, &failedJobCreationDetails{
				JobRunMeta: jobMeta,
				Error:      err,
			})
		} else {
			submitJobs = append(submitJobs, submitJob)
		}
	}

	return submitJobs, failedJobCreations
}

func extractEssentialJobMetadata(jobRun *executorapi.JobRunLease) (*job.RunMetaInfo, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(jobRun.Job.JobId)
	if err != nil {
		return nil, fmt.Errorf("unable to extract jobId because %s", err)
	}
	runId, err := armadaevents.UuidStringFromProtoUuid(jobRun.JobRunId)
	if err != nil {
		return nil, fmt.Errorf("unable to extract runId because %s", err)
	}
	if jobRun.Queue == "" {
		return nil, fmt.Errorf("job is invalid, queue is empty")
	}
	if jobRun.Jobset == "" {
		return nil, fmt.Errorf("job is invalid, jobset is empty")
	}

	return &job.RunMetaInfo{
		JobId:  jobId,
		RunId:  runId,
		Queue:  jobRun.Queue,
		JobSet: jobRun.Jobset,
	}, nil
}

func (allocationService *ClusterAllocationService) handleFailedJobCreation(failedJobCreationDetails []*failedJobCreationDetails) {
	for _, failedCreateDetails := range failedJobCreationDetails {
		failedEvent := &api.JobFailedEvent{
			JobId:             failedCreateDetails.JobRunMeta.JobId,
			JobSetId:          failedCreateDetails.JobRunMeta.JobSet,
			Queue:             failedCreateDetails.JobRunMeta.Queue,
			Created:           time.Now(),
			ClusterId:         allocationService.clusterContext.GetClusterId(),
			Reason:            failedCreateDetails.Error.Error(),
			ExitCodes:         map[string]int32{},
			ContainerStatuses: []*api.ContainerStatus{},
			Cause:             api.Cause_Error,
		}
		err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: failedCreateDetails.JobRunMeta.RunId}})
		if err == nil {
			allocationService.jobRunStateManager.ReportFailedSubmission(failedCreateDetails.JobRunMeta)
		} else {
			// This will cause us to lease it again - which is acceptable as the pod was never created
			allocationService.jobRunStateManager.Delete(failedCreateDetails.JobRunMeta.RunId)
			log.Errorf("Failed to report job creation failed for job %s (run id %s) because %s",
				failedCreateDetails.JobRunMeta.JobId, failedCreateDetails.JobRunMeta.RunId, err)
		}
	}
}

// Returns the RunIds of all managed pods that haven't been assigned to a node
func (allocationService *ClusterAllocationService) getUnassignedRunIds(capacityReport *utilisation.ClusterAvailableCapacityReport) ([]armadaevents.Uuid, error) {
	allAssignedRunIds := []string{}
	allJobRunIds := []string{}

	for _, node := range capacityReport.Nodes {
		allAssignedRunIds = append(allAssignedRunIds, maps.Keys(node.RunIdsByState)...)
	}

	// We make the assumption here that JobRunStateManager knows about all job runs and don't reconcile again against kubernetes
	// This should be a safe assumption - and would be a bug if it was ever not true
	allJobRuns := allocationService.jobRunStateManager.GetAll()
	allJobRunIds = append(allJobRunIds, slices.Map(allJobRuns, func(val *job.RunState) string {
		return val.Meta.RunId
	})...)

	unassignedIds := slices.Subtract(allJobRunIds, allAssignedRunIds)
	unassignedIds = slices.Filter(unassignedIds, func(val string) bool {
		return val != ""
	})

	return util.StringUuidsToUuids(unassignedIds)
}

func (allocationService *ClusterAllocationService) processFailedJobSubmissions(failedSubmissions []*job.FailedSubmissionDetails) {
	for _, details := range failedSubmissions {
		message := details.Error.Error()
		if apiError, ok := details.Error.(errors.APIStatus); ok {
			message = apiError.Status().Message
		}

		if details.Recoverable {
			returnLeaseEvent := reporter.CreateReturnLeaseEvent(details.Pod, message, allocationService.clusterContext.GetClusterId(), true)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: returnLeaseEvent, JobRunId: details.JobRunMeta.RunId}})
			if err == nil {
				allocationService.jobRunStateManager.ReportFailedSubmission(details.JobRunMeta)
			} else {
				// This will cause us to lease it again - which is acceptable as the pod was never created
				allocationService.jobRunStateManager.Delete(details.JobRunMeta.RunId)
				log.Errorf("Failed to return lease for job %s because %s", details.JobRunMeta.JobId, err)
			}
		} else {
			failEvent := reporter.CreateSimpleJobFailedEvent(details.Pod, message, allocationService.clusterContext.GetClusterId(), api.Cause_Error)
			err := allocationService.eventReporter.Report([]reporter.EventMessage{{Event: failEvent, JobRunId: details.JobRunMeta.RunId}})
			if err == nil {
				allocationService.jobRunStateManager.ReportFailedSubmission(details.JobRunMeta)
			} else {
				// This will cause us to lease it again - which is acceptable as the pod was never created
				allocationService.jobRunStateManager.Delete(details.JobRunMeta.RunId)
				log.Errorf("Failed to report submission as failed for job %s because %s", details.JobRunMeta.JobId, err)
			}
		}
	}
}

func (allocationService *ClusterAllocationService) processRunsToCancel(runsToRemove []*armadaevents.Uuid) {
	runsToRemoveIds := make([]string, 0, len(runsToRemove))
	for _, runToCancelId := range runsToRemove {
		runIdStr, err := armadaevents.UuidStringFromProtoUuid(runToCancelId)
		if err != nil {
			log.Errorf("Skipping removing run because %s", err)
			continue
		}
		runsToRemoveIds = append(runsToRemoveIds, runIdStr)
	}
	runsToRemoveSet := util2.StringListToSet(runsToRemoveIds)

	managedPods, err := allocationService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to cancel runs because unable to get a current managed pods due to %s", err)
		return
	}

	// Find all runs with a pod
	podsToRemove := make([]*v1.Pod, 0, len(runsToRemove))
	for _, pod := range managedPods {
		runId := util.ExtractJobRunId(pod)
		if _, ok := runsToRemoveSet[runId]; ok {
			podsToRemove = append(podsToRemove, pod)
		}
	}

	// Annotate corresponding pods with JobDoneAnnotation
	// Then update the runs state
	util.ProcessItemsWithThreadPool(context.Background(), 20, podsToRemove,
		func(pod *v1.Pod) {
			if util.IsInTerminalState(pod) && util.HasCurrentStateBeenReported(pod) {
				if !util.IsReportedDone(pod) {
					err := allocationService.clusterContext.AddAnnotation(pod, map[string]string{
						domain.JobDoneAnnotation: time.Now().String(),
					})
					if err != nil {
						log.Errorf("Failed to annotate pod %s as done because %s", pod.Name, err)
						return
					}
				}

				runId := util.ExtractJobRunId(pod)
				allocationService.jobRunStateManager.Delete(runId)
			} else {
				// This path should only happen during cancellation
				allocationService.clusterContext.DeletePods(podsToRemove)
			}
		},
	)
	// For all runs that don't have a corresponding pod, delete the run from the state
	runsWithPods := slices.Map(podsToRemove, func(pod *v1.Pod) string {
		return util.ExtractJobRunId(pod)
	})

	runsToDelete := slices.Subtract(runsToRemoveIds, runsWithPods)

	for _, runToDelete := range runsToDelete {
		allocationService.jobRunStateManager.Delete(runToDelete)
	}
}

type LegacyClusterAllocationService struct {
	leaseService       LeaseService
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterContext     executorContext.ClusterContext
	submitter          job.Submitter
	etcdHealthMonitor  healthmonitor.EtcdLimitHealthMonitor
	reserved           armadaresource.ComputeResources
}

func NewLegacyClusterAllocationService(
	clusterContext executorContext.ClusterContext,
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
