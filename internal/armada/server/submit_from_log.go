package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/events"
	"github.com/G-Research/armada/pkg/api"
)

// LogToArmada is a service that reads messages from Pulsar and updates the state of the Armada server accordingly.
// Calls into an embedded Armada submit server object.
type SubmitFromLog struct {
	submitServer SubmitServer
}

func (srv *SubmitFromLog) SubmitJobs(ctx context.Context, userId string, groups []string, queueName string, jobSetName string, es []*events.SubmitJob) error {

	jobs, err := apiJobsFromLogSubmitJobs(userId, groups, queueName, jobSetName, time.Now(), es)
	if err != nil {
		return err
	}

	// Submit the jobs by writing them to the database.
	submissionResults, err := srv.submitServer.jobRepository.AddJobs(jobs)
	if err != nil {
		jobFailures := createJobFailuresWithReason(jobs, fmt.Sprintf("Failed to save job in Armada: %s", err))
		reportErr := reportFailed(srv.submitServer.eventStore, "", jobFailures)
		if reportErr != nil {
			return status.Errorf(codes.Internal, "[SubmitJobs] error reporting failure event: %v", reportErr)
		}
		return status.Errorf(codes.Aborted, "[SubmitJobs] error saving jobs in Armada: %s", err)
	}

	// Create events that report what happened.
	// Later, this will be changed to submit log messages.
	var createdJobs []*api.Job
	var jobFailures []*jobFailure
	var doubleSubmits []*repository.SubmitJobResult
	for i, submissionResult := range submissionResults {
		jobResponse := &api.JobSubmitResponseItem{JobId: submissionResult.JobId}

		if submissionResult.Error != nil {
			jobResponse.Error = submissionResult.Error.Error()
			jobFailures = append(jobFailures, &jobFailure{
				job:    jobs[i],
				reason: fmt.Sprintf("Failed to save job in Armada: %s", submissionResult.Error.Error()),
			})
		} else if submissionResult.DuplicateDetected {
			doubleSubmits = append(doubleSubmits, submissionResult)
		} else {
			createdJobs = append(createdJobs, jobs[i])
		}
	}

	err = reportFailed(srv.submitServer.eventStore, "", jobFailures)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting failed jobs: %s", err))
	}

	err = reportDuplicateDetected(srv.submitServer.eventStore, doubleSubmits)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting duplicate jobs: %s", err))
	}

	err = reportQueued(srv.submitServer.eventStore, createdJobs)
	if err != nil {
		return status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting queued jobs: %s", err))
	}

	return nil
}

func apiJobsFromLogSubmitJobs(userId string, groups []string, queueName string, jobSetName string, time time.Time, es []*events.SubmitJob) ([]*api.Job, error) {
	jobs := make([]*api.Job, len(es), len(es))
	for i, e := range es {
		job, err := apiJobFromLogSubmitJob(userId, groups, queueName, jobSetName, time, e)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	return jobs, nil
}

// apiJobFromLogSubmitJob converts a SubmitJob log message into an api.Job struct, which is used by Armada internally.
func apiJobFromLogSubmitJob(ownerId string, groups []string, queueName string, jobSetName string, time time.Time, e *events.SubmitJob) (*api.Job, error) {

	// The log submit API only supports a single pod spec. per job, which is placed in the main object.
	mainObject, ok := e.MainObject.Object.(*events.KubernetesMainObject_PodSpec)
	if !ok {
		return nil, errors.Errorf("expected *PodSpecWithAvoidList, but got %v", e.MainObject.Object)
	}
	podSpec := mainObject.PodSpec.PodSpec

	// The job submit message contains a bag of additional k8s objects to create as part of the job.
	// We need to separate those out into service and ingress types.
	k8sServices := make([]*v1.Service, 0)
	k8sIngresses := make([]*networking.Ingress, 0)
	for _, object := range e.Objects {
		switch o := object.Object.(type) {
		case *events.KubernetesObject_Service:
			k8sServices = append(k8sServices, o.Service)
		case *events.KubernetesObject_Ingress:
			k8sIngresses = append(k8sIngresses, o.Ingress)
		case *events.KubernetesObject_PodSpec:
			return nil, &armadaerrors.ErrInvalidArgument{
				Name:    "Objects",
				Value:   o,
				Message: "providing more than one pod spec isn't supported",
			}
		default:
			return nil, &armadaerrors.ErrInvalidArgument{
				Name:    "Objects",
				Value:   o,
				Message: "unsupported k8s object",
			}
		}
	}

	return &api.Job{
		Id:       e.JobId,
		ClientId: e.DeduplicationId,
		Queue:    queueName,
		JobSetId: jobSetName,

		Namespace:   e.Namespace,
		Labels:      e.Labels,
		Annotations: e.Annotations,

		K8SIngress:         k8sIngresses,
		K8SService:         k8sServices,

		Priority: e.Priority,

		PodSpec:                  podSpec,
		PodSpecs:                 make([]*v1.PodSpec, 0),
		Created:                  time,
		Owner:                    ownerId,
		QueueOwnershipUserGroups: groups,
	}, nil
}

// CancelJobs cancels all jobs specified by the provided events in a single operation.
func (srv *SubmitFromLog) CancelJobs(ctx context.Context, userName string, es []*events.CancelJob) error {
	jobIds := make([]string, len(es), len(es))
	for i, e := range es {
		jobIds[i] = e.JobId
	}
	_, err := srv.CancelJobsById(ctx, userName, jobIds)
	return err
}

func (srv *SubmitFromLog) CancelJobSet(ctx context.Context, queueName string, jobSetName string) error {

	jobIds, err := srv.submitServer.jobRepository.GetActiveJobIds(queueName, jobSetName)
	if err != nil {
		return err
	}

	// Split IDs into batches and process one batch at a time.
	// To reduce the number of jobs stored in memory.
	jobIdBatches := util.Batch(jobIds, srv.submitServer.cancelJobsBatchSize)
	for _, jobIdBatch := range jobIdBatches {
		_, err := srv.CancelJobsById(ctx, queueName, jobIdBatch)
		if err != nil {
			return err
		}

		// TODO I think the right way to do this is to include a timeout with the call to Redis
		// Then, we can check for a deadline exceeded error here
		if util.CloseToDeadline(ctx, time.Second*1) {
			err = errors.Errorf("deadline exceeded")
			return errors.WithStack(err)
		}
	}

	return nil
}

// CancelJobsById cancels all jobs with the specified ids.
func (srv *SubmitFromLog) CancelJobsById(ctx context.Context, userName string, jobIds []string) ([]string, error) {

	jobs, err := srv.submitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return nil, err
	}

	err = reportJobsCancelling(srv.submitServer.eventStore, userName, jobs)
	if err != nil {
		return nil, err
	}

	deletionResult, err := srv.submitServer.jobRepository.DeleteJobs(jobs)
	if err != nil {
		return nil, err
	}

	cancelled := []*api.Job{}
	cancelledIds := []string{}
	for job, err := range deletionResult {
		if err != nil {
			// TODO: Use sibling errors.
			log.Errorf("[cancelJobs] error cancelling job with ID %s: %s", job.Id, err)
		} else {
			cancelled = append(cancelled, job)
			cancelledIds = append(cancelledIds, job.Id)
		}
	}

	err = reportJobsCancelled(srv.submitServer.eventStore, userName, cancelled)
	if err != nil {
		return nil, err
	}

	return cancelledIds, nil
}

// ReprioritizeJobs updates the priority of one of more jobs.
func (srv *SubmitFromLog) ReprioritizeJobs(ctx context.Context, userName string, es []*events.ReprioritiseJob) error {
	if len(es) == 0 {
		return nil
	}

	jobIds := make([]string, len(es), len(es))
	for i, e := range es {
		jobIds[i] = e.JobId
	}
	jobs, err := srv.submitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return err
	}

	// The submit API guarantees that all events specify the same priority.
	newPriority := es[0].Priority
	for _, e := range es {
		if e.Priority != newPriority {
			err = errors.Errorf("all ReprioritiseJob events must have the same priority")
			return errors.WithStack(err)
		}
	}

	err = reportJobsReprioritizing(srv.submitServer.eventStore, userName, jobs, newPriority)
	if err != nil {
		return err
	}

	_, err = srv.submitServer.reprioritizeJobs(jobIds, newPriority, userName)
	if err != nil {
		return err
	}

	return nil
}

func (srv *SubmitFromLog) ReprioritizeJobSet(ctx context.Context, userName string, queueName string, jobSetName string, e *events.ReprioritiseJobSet) error {
	jobIds, err := srv.submitServer.jobRepository.GetActiveJobIds(queueName, jobSetName)
	if err != nil {
		return err
	}
	jobs, err := srv.submitServer.jobRepository.GetExistingJobsByIds(jobIds)
	if err != nil {
		return err
	}

	err = reportJobsReprioritizing(srv.submitServer.eventStore, userName, jobs, e.Priority)
	if err != nil {
		return err
	}

	_, err = srv.submitServer.reprioritizeJobs(jobIds, e.Priority, userName)
	if err != nil {
		return err
	}

	return nil
}