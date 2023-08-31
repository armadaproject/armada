package service

import (
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/internal/common/slices"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type JobRequester struct {
	leaseRequester     LeaseRequester
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterId          executorContext.ClusterIdentity
	podDefaults        *configuration.PodDefaults
	jobRunStateStore   job.RunStateStore
}

func NewJobRequester(
	clusterId executorContext.ClusterIdentity,
	eventReporter reporter.EventReporter,
	leaseRequester LeaseRequester,
	jobRunStateStore job.RunStateStore,
	utilisationService utilisation.UtilisationService,
	podDefaults *configuration.PodDefaults,
) *JobRequester {
	return &JobRequester{
		leaseRequester:     leaseRequester,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		jobRunStateStore:   jobRunStateStore,
		clusterId:          clusterId,
		podDefaults:        podDefaults,
	}
}

func (r *JobRequester) RequestJobsRuns() {
	leaseRequest, err := r.createLeaseRequest()
	if err != nil {
		log.Errorf("Failed to create lease request because %s", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leaseResponse, err := r.leaseRequester.LeaseJobRuns(ctx, leaseRequest)
	if err != nil {
		log.Errorf("Failed to request new jobs leases as because %s", err)
		return
	}
	logAvailableResources(leaseRequest.AvailableResource, len(leaseResponse.LeasedRuns))

	jobs, failedJobCreations := r.createSubmitJobs(leaseResponse.LeasedRuns)
	r.markJobRunsAsLeased(jobs)
	r.markJobRunsAsCancelled(leaseResponse.RunIdsToCancel)
	r.markJobRunsToPreempt(leaseResponse.RunIdsToPreempt)
	r.handleFailedJobCreation(failedJobCreations)
}

func (r *JobRequester) createLeaseRequest() (*LeaseRequest, error) {
	capacityReport, err := r.utilisationService.GetAvailableClusterCapacity(false)
	if err != nil {
		return nil, err
	}

	unassignedRunIds, err := r.getUnassignedRunIds(capacityReport)
	if err != nil {
		return nil, err
	}

	nodes := make([]*api.NodeInfo, 0, len(capacityReport.Nodes))
	for i := range capacityReport.Nodes {
		nodes = append(nodes, &capacityReport.Nodes[i])
	}

	return &LeaseRequest{
		AvailableResource:   *capacityReport.AvailableCapacity,
		Nodes:               nodes,
		UnassignedJobRunIds: unassignedRunIds,
	}, nil
}

// Returns the RunIds of all managed pods that haven't been assigned to a node
func (r *JobRequester) getUnassignedRunIds(capacityReport *utilisation.ClusterAvailableCapacityReport) ([]armadaevents.Uuid, error) {
	allAssignedRunIds := []string{}
	allJobRunIds := []string{}

	for _, node := range capacityReport.Nodes {
		allAssignedRunIds = append(allAssignedRunIds, maps.Keys(node.RunIdsByState)...)
	}

	// We make the assumption here that JobRunStateStore knows about all job runs and don't reconcile again against kubernetes
	// This should be a safe assumption - and would be a bug if it was ever not true
	allJobRuns := r.jobRunStateStore.GetAll()
	allJobRunIds = append(allJobRunIds, util2.Map(allJobRuns, func(val *job.RunState) string {
		return val.Meta.RunId
	})...)

	unassignedIds := slices.Subtract(allJobRunIds, allAssignedRunIds)

	return util.StringUuidsToUuids(unassignedIds)
}

type failedJobCreationDetails struct {
	JobRunMeta *job.RunMeta
	Error      error
}

func (r *JobRequester) createSubmitJobs(newJobRuns []*executorapi.JobRunLease) ([]*job.SubmitJob, []*failedJobCreationDetails) {
	submitJobs := make([]*job.SubmitJob, 0, len(newJobRuns))
	failedJobCreations := []*failedJobCreationDetails{}
	for _, jobToSubmit := range newJobRuns {
		jobMeta, err := ExtractEssentialJobMetadata(jobToSubmit)
		if err != nil {
			log.Errorf("received invalid job - %s", err)
			continue
		}

		submitJob, err := job.CreateSubmitJobFromExecutorApiJobRunLease(jobToSubmit, r.podDefaults)
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

func (r *JobRequester) markJobRunsAsLeased(jobs []*job.SubmitJob) {
	for _, j := range jobs {
		r.jobRunStateStore.ReportRunLeased(j.Meta.RunMeta, j)
	}
}

func (r *JobRequester) markJobRunsAsCancelled(runIdsToCancel []*armadaevents.Uuid) {
	for _, runToCancelId := range runIdsToCancel {
		runIdStr, err := armadaevents.UuidStringFromProtoUuid(runToCancelId)
		if err != nil {
			log.Errorf("Skipping removing run because %s", err)
			continue
		}
		r.jobRunStateStore.RequestRunCancellation(runIdStr)
	}
}

func (r *JobRequester) markJobRunsToPreempt(runIdsToPreempt []*armadaevents.Uuid) {
	for _, runToCancelId := range runIdsToPreempt {
		runIdStr, err := armadaevents.UuidStringFromProtoUuid(runToCancelId)
		if err != nil {
			log.Errorf("Skipping preempting run because %s", err)
			continue
		}
		r.jobRunStateStore.RequestRunPreemption(runIdStr)
	}
}

func (r *JobRequester) handleFailedJobCreation(failedJobCreationDetails []*failedJobCreationDetails) {
	for _, failedCreateDetails := range failedJobCreationDetails {
		failedEvent := &api.JobFailedEvent{
			JobId:             failedCreateDetails.JobRunMeta.JobId,
			JobSetId:          failedCreateDetails.JobRunMeta.JobSet,
			Queue:             failedCreateDetails.JobRunMeta.Queue,
			Created:           time.Now(),
			ClusterId:         r.clusterId.GetClusterId(),
			Reason:            failedCreateDetails.Error.Error(),
			ExitCodes:         map[string]int32{},
			ContainerStatuses: []*api.ContainerStatus{},
			Cause:             api.Cause_Error,
		}
		err := r.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: failedCreateDetails.JobRunMeta.RunId}})
		if err == nil {
			r.jobRunStateStore.ReportRunInvalid(failedCreateDetails.JobRunMeta)
		} else {
			log.Errorf("Failed to report job creation failed for job %s (run id %s) because %s",
				failedCreateDetails.JobRunMeta.JobId, failedCreateDetails.JobRunMeta.RunId, err)
		}
	}
}
