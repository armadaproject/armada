package service

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/executor/configuration"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type JobRequester struct {
	leaseRequester     LeaseRequester
	eventReporter      reporter.EventReporter
	utilisationService utilisation.UtilisationService
	clusterId          executorContext.ClusterIdentity
	podDefaults        *configuration.PodDefaults
	jobRunStateStore   job.RunStateStore
	maxLeasedJobs      int
}

func NewJobRequester(
	clusterId executorContext.ClusterIdentity,
	eventReporter reporter.EventReporter,
	leaseRequester LeaseRequester,
	jobRunStateStore job.RunStateStore,
	utilisationService utilisation.UtilisationService,
	podDefaults *configuration.PodDefaults,
	maxLeasedJobs int,
) *JobRequester {
	return &JobRequester{
		leaseRequester:     leaseRequester,
		eventReporter:      eventReporter,
		utilisationService: utilisationService,
		jobRunStateStore:   jobRunStateStore,
		clusterId:          clusterId,
		podDefaults:        podDefaults,
		maxLeasedJobs:      maxLeasedJobs,
	}
}

func (r *JobRequester) RequestJobsRuns() {
	leaseRequest, err := r.createLeaseRequest()
	if err != nil {
		log.Errorf("Failed to create lease request because %s", err)
		return
	}
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	leaseResponse, err := r.leaseRequester.LeaseJobRuns(ctx, leaseRequest)
	if err != nil {
		log.Errorf("Failed to request new jobs leases as because %s", err)
		return
	}
	log.Infof("Reporting current free resource %s. Requesting %d new jobs. Received %d new jobs.",
		formatResources(leaseRequest.AvailableResource), leaseRequest.MaxJobsToLease, len(leaseResponse.LeasedRuns))

	jobs, failedJobCreations := r.createSubmitJobs(leaseResponse.LeasedRuns)
	r.markJobRunsAsLeased(jobs)
	r.markJobRunsAsCancelled(leaseResponse.RunIdsToCancel)
	r.markJobRunsToPreempt(leaseResponse.RunIdsToPreempt)
	r.handleFailedJobCreation(failedJobCreations)
}

func (r *JobRequester) createLeaseRequest() (*LeaseRequest, error) {
	capacityReport, err := r.utilisationService.GetAvailableClusterCapacity()
	if err != nil {
		return nil, err
	}

	unassignedRunIds := r.getUnassignedRunIds(capacityReport)
	nodes := make([]*executorapi.NodeInfo, 0, len(capacityReport.Nodes))
	for i := range capacityReport.Nodes {
		nodes = append(nodes, &capacityReport.Nodes[i])
	}

	leasedJobs := r.jobRunStateStore.GetAllWithFilter(func(state *job.RunState) bool { return state.Phase == job.Leased })
	maxJobsToLease := r.maxLeasedJobs
	if len(leasedJobs) > 0 {
		maxJobsToLease = 0
	}

	return &LeaseRequest{
		AvailableResource:   *capacityReport.AvailableCapacity,
		Nodes:               nodes,
		UnassignedJobRunIds: unassignedRunIds,
		MaxJobsToLease:      uint32(maxJobsToLease),
	}, nil
}

// Returns the RunIds of all managed pods that haven't been assigned to a node
func (r *JobRequester) getUnassignedRunIds(capacityReport *utilisation.ClusterAvailableCapacityReport) []string {
	allAssignedRunIds := []string{}
	allJobRunIds := []string{}

	for _, node := range capacityReport.Nodes {
		allAssignedRunIds = append(allAssignedRunIds, maps.Keys(node.RunIdsByState)...)
	}

	// We make the assumption here that JobRunStateStore knows about all job runs and don't reconcile again against kubernetes
	// This should be a safe assumption - and would be a bug if it was ever not true
	allJobRuns := r.jobRunStateStore.GetAll()
	allJobRunIds = append(allJobRunIds, slices.Map(allJobRuns, func(val *job.RunState) string {
		return val.Meta.RunId
	})...)

	unassignedIds := slices.Subtract(allJobRunIds, allAssignedRunIds)

	return unassignedIds
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

func (r *JobRequester) markJobRunsAsCancelled(runIdsToCancel []string) {
	for _, runId := range runIdsToCancel {
		r.jobRunStateStore.RequestRunCancellation(runId)
	}
}

func (r *JobRequester) markJobRunsToPreempt(runIdsToPreempt []string) {
	for _, runId := range runIdsToPreempt {
		r.jobRunStateStore.RequestRunPreemption(runId)
	}
}

func (r *JobRequester) handleFailedJobCreation(failedJobCreationDetails []*failedJobCreationDetails) {
	for _, failedCreateDetails := range failedJobCreationDetails {
		err := r.sendFailedEvent(failedCreateDetails)
		if err == nil {
			r.jobRunStateStore.ReportRunInvalid(failedCreateDetails.JobRunMeta)
		} else {
			log.Errorf("Failed to report job creation failed for job %s (run id %s) because %s",
				failedCreateDetails.JobRunMeta.JobId, failedCreateDetails.JobRunMeta.RunId, err)
		}
	}
}

func (r *JobRequester) sendFailedEvent(details *failedJobCreationDetails) error {
	failedEvent, err := reporter.CreateMinimalJobFailedEvent(
		details.JobRunMeta.JobId,
		details.JobRunMeta.RunId,
		details.JobRunMeta.JobSet,
		details.JobRunMeta.Queue,
		r.clusterId.GetClusterId(),
		details.Error.Error(),
	)
	if err != nil {
		return fmt.Errorf("failed to create job failed events because %s", err)
	}
	return r.eventReporter.Report([]reporter.EventMessage{{Event: failedEvent, JobRunId: details.JobRunMeta.RunId}})
}
