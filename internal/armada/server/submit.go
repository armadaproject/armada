package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/pkg/api"
)

type SubmitServer struct {
	permissions              authorization.PermissionChecker
	jobRepository            repository.JobRepository
	queueRepository          repository.QueueRepository
	eventStore               repository.EventStore
	schedulingInfoRepository repository.SchedulingInfoRepository
	cancelJobsBatchSize      int
	queueManagementConfig    *configuration.QueueManagementConfig
	schedulingConfig         *configuration.SchedulingConfig
}

func NewSubmitServer(
	permissions authorization.PermissionChecker,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	cancelJobsBatchSize int,
	queueManagementConfig *configuration.QueueManagementConfig,
	schedulingConfig *configuration.SchedulingConfig,
) *SubmitServer {

	return &SubmitServer{
		permissions:              permissions,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository,
		cancelJobsBatchSize:      cancelJobsBatchSize,
		queueManagementConfig:    queueManagementConfig,
		schedulingConfig:         schedulingConfig}
}

func (server *SubmitServer) GetQueueInfo(ctx context.Context, req *api.QueueInfoRequest) (*api.QueueInfo, error) {
	err := checkPermission(server.permissions, ctx, permissions.WatchAllEvents)
	var e *ErrNoPermission
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.PermissionDenied, "[GetQueueInfo] error for queue %s: %s", req.Name, e)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueueInfo] error checking permissions: %s", err)
	}

	jobSets, err := server.jobRepository.GetQueueActiveJobSets(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueueInfo] error getting job sets for queue %s: %s", req.Name, err)
	}

	result := &api.QueueInfo{
		Name:          req.Name,
		ActiveJobSets: jobSets,
	}
	return result, nil
}

func (server *SubmitServer) GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	queue, err := server.queueRepository.GetQueue(req.Name)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[GetQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueue] error getting queue %q: %s", req.Name, err)
	}
	return queue, nil
}

func (server *SubmitServer) CreateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
	err := checkPermission(server.permissions, ctx, permissions.CreateQueue)
	var ep *ErrNoPermission
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", queue.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
	}

	if len(queue.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		queue.UserOwners = []string{principal.GetName()}
	}

	if err := validateQueue(queue); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error validating queue: %s", err)
	}

	err = server.queueRepository.CreateQueue(queue)
	var eq *repository.ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "[CreateQueue] error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (server *SubmitServer) UpdateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
	err := checkPermission(server.permissions, ctx, permissions.CreateQueue)
	var ep *ErrNoPermission
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[UpdateQueue] error updating queue %s: %s", queue.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error checking permissions: %s", err)
	}

	if err := validateQueue(queue); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	err = server.queueRepository.UpdateQueue(queue)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[UpdateQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func validateQueue(queue *api.Queue) error {
	if queue.PriorityFactor < 1.0 {
		return fmt.Errorf("[validateQueue] queue priority must be greater than or equal to 1, but is %f", queue.PriorityFactor)
	}
	return nil
}

func (server *SubmitServer) DeleteQueue(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
	err := checkPermission(server.permissions, ctx, permissions.DeleteQueue)
	var ep *ErrNoPermission
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[DeleteQueue] error deleting queue %s: %s", request.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[DeleteQueue] error checking permissions: %s", err)
	}

	active, err := server.jobRepository.GetQueueActiveJobSets(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error getting active job sets for queue %s: %s", request.Name, err)
	}
	if len(active) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "[DeleteQueue] error deleting queue %s: queue is not empty", request.Name)
	}

	err = server.queueRepository.DeleteQueue(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error deleting queue %s: %s", request.Name, err)
	}

	return &types.Empty{}, nil
}

func (server *SubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ownershipGroups, err := server.checkQueuePermission(ctx, req.Queue, true, permissions.SubmitJobs, permissions.SubmitAnyJobs)
	var e *ErrNoPermission
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.PermissionDenied, "[SubmitJobs] error submitting jobs: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[SubmitJobs] error checking permissions: %s", err)
	}

	// Creates objects representing the jobs without scheduling them to run
	principal := authorization.GetPrincipal(ctx)
	jobs, err := server.createJobs(req, principal.GetName(), ownershipGroups)
	if err != nil {
		// TODO Should we log the JSON? We need to be careful about handling parsing errors.
		return nil, status.Errorf(codes.InvalidArgument, "[SubmitJobs] error creating jobs for user %s: %s", principal.GetName(), err)
	}

	// Check if the job would fit on any executor,
	// to avoid having users wait for a job that may never be scheduled
	allClusterSchedulingInfo, err := server.schedulingInfoRepository.GetClusterSchedulingInfo()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[SubmitJobs] error getting scheduling info: %s", err)
	}

	err = validateJobsCanBeScheduled(jobs, allClusterSchedulingInfo)
	if err != nil {
		// TODO Should we log the JSON? We need to be careful about handling parsing errors.
		return nil, status.Errorf(codes.InvalidArgument, "[SubmitJobs] error submitting jobs for user %s: %s", principal.GetName(), err)
	}

	// Create events marking the jobs as submitted
	err = reportSubmitted(server.eventStore, jobs)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "[SubmitJobs] error getting submitted report: %s", err)
	}

	// Submit the jobs by writing them to the database
	submissionResults, err := server.jobRepository.AddJobs(jobs)
	if err != nil {
		jobFailures := createJobFailuresWithReason(jobs, fmt.Sprintf("Failed to save job in Armada: %s", e))
		reportErr := reportFailed(server.eventStore, "", jobFailures)
		if reportErr != nil {
			return nil, status.Errorf(codes.Internal, "[SubmitJobs] error reporting failure event: %v", reportErr)
		}
		return nil, status.Errorf(codes.Aborted, "[SubmitJobs] error saving jobs in Armada: %s", err)
	}

	// Create the response to send to the client
	result := &api.JobSubmitResponse{
		JobResponseItems: make([]*api.JobSubmitResponseItem, 0, len(submissionResults)),
	}

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

		result.JobResponseItems = append(result.JobResponseItems, jobResponse)
	}

	err = reportFailed(server.eventStore, "", jobFailures)
	if err != nil {
		return result, status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting failed jobs: %s", err))
	}

	err = reportDuplicateDetected(server.eventStore, doubleSubmits)
	if err != nil {
		return result, status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting duplicate jobs: %s", err))
	}

	err = reportQueued(server.eventStore, createdJobs)
	if err != nil {
		return result, status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error reporting queued jobs: %s", err))
	}

	if len(jobFailures) > 0 {
		return result, status.Errorf(codes.Internal, fmt.Sprintf("[SubmitJobs] error submitting some or all jobs: %s", err))
	}

	return result, nil
}

// CancelJobs cancels jobs identified by the request.
// If the request contains a job ID, only the job with that ID is cancelled.
// If the request contains a queue name and a job set ID, all jobs matching those are cancelled.
func (server *SubmitServer) CancelJobs(ctx context.Context, request *api.JobCancelRequest) (*api.CancellationResult, error) {
	if request.JobId != "" {
		return server.cancelJobsById(ctx, request.JobId)
	} else if request.JobSetId != "" && request.Queue != "" {
		return server.cancelJobsByQueueAndSet(ctx, request.Queue, request.JobSetId)
	}
	return nil, status.Errorf(codes.InvalidArgument, "[CancelJobs] specify either job ID or both queue name and job set ID")
}

// cancels a job with a given ID
func (server *SubmitServer) cancelJobsById(ctx context.Context, jobId string) (*api.CancellationResult, error) {
	jobs, err := server.jobRepository.GetExistingJobsByIds([]string{jobId})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[cancelJobsById] error getting job with ID %s: %s", jobId, err)
	}
	if len(jobs) != 1 {
		return nil, status.Errorf(codes.Internal, "[cancelJobsById] error getting job with ID %s: expected exactly one result, but got %v", jobId, jobs)
	}

	result, err := server.cancelJobs(ctx, jobs[0].Queue, jobs)
	var e *ErrNoPermission
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.PermissionDenied, "[cancelJobsById] error canceling job with ID %s: %s", jobId, e)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[cancelJobsById] error checking permissions: %s", err)
	}

	return result, nil
}

// cancels all jobs part of a particular job set and queue
//
// TODO Should we cancel as many jobs as we can instead of returning on error?
func (server *SubmitServer) cancelJobsByQueueAndSet(ctx context.Context, queue string, jobSetId string) (*api.CancellationResult, error) {
	ids, err := server.jobRepository.GetActiveJobIds(queue, jobSetId)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[cancelJobsBySetAndQueue] error getting job IDs: %s", err)
	}

	// Split IDs into batches and process one batch at a time
	// To reduce the number of jobs stored in memory
	batches := util.Batch(ids, server.cancelJobsBatchSize)
	cancelledIds := []string{}
	for _, batch := range batches {
		jobs, err := server.jobRepository.GetExistingJobsByIds(batch)
		if err != nil {
			// TODO Let's have a ErrJobNotFound we can check for here and return the NotFound error code
			result := &api.CancellationResult{CancelledIds: cancelledIds}
			return result, status.Errorf(codes.Internal, "[cancelJobsBySetAndQueue] could not find some jobs: %s", err)
		}

		result, err := server.cancelJobs(ctx, queue, jobs)
		var e *ErrNoPermission
		if errors.As(err, &e) {
			return nil, status.Errorf(codes.PermissionDenied, "[cancelJobsBySetAndQueue] error canceling jobs: %s", e)
		} else if err != nil {
			result := &api.CancellationResult{CancelledIds: cancelledIds}
			return result, status.Errorf(codes.Unavailable, "[cancelJobsBySetAndQueue] error checking permissions: %s", err)
		}
		cancelledIds = append(cancelledIds, result.CancelledIds...)

		// TODO I think the right way to do this is to include a timeout with the call to Redis
		// Then, we can check for a deadline exceeded error here
		if util.CloseToDeadline(ctx, time.Second*1) {
			result := &api.CancellationResult{CancelledIds: cancelledIds}
			return result, status.Errorf(codes.DeadlineExceeded, "[cancelJobsBySetAndQueue] deadline exceeded")
		}
	}

	return &api.CancellationResult{CancelledIds: cancelledIds}, nil
}

func (server *SubmitServer) cancelJobs(ctx context.Context, queue string, jobs []*api.Job) (*api.CancellationResult, error) {
	if _, err := server.checkQueuePermission(ctx, queue, false, permissions.CancelJobs, permissions.CancelAnyJobs); err != nil {
		return nil, fmt.Errorf("[cancelJobs] error checking permissions: %w", err)
	}
	principal := authorization.GetPrincipal(ctx)

	err := reportJobsCancelling(server.eventStore, principal.GetName(), jobs)
	if err != nil {
		return nil, fmt.Errorf("[cancelJobs] error reporting jobs marked as cancelled: %w", err)
	}

	deletionResult, err := server.jobRepository.DeleteJobs(jobs)
	if err != nil {
		return nil, fmt.Errorf("[cancelJobs] error deleting jobs: %w", err)
	}
	cancelled := []*api.Job{}
	cancelledIds := []string{}
	for job, err := range deletionResult {
		if err != nil {
			log.Errorf("[cancelJobs] error cancelling job with ID %s: %s", job.Id, err)
		} else {
			cancelled = append(cancelled, job)
			cancelledIds = append(cancelledIds, job.Id)
		}
	}

	err = reportJobsCancelled(server.eventStore, principal.GetName(), cancelled)
	if err != nil {
		return nil, fmt.Errorf("[cancelJobs] error reporting job cancellation: %w", err)
	}

	return &api.CancellationResult{CancelledIds: cancelledIds}, nil
}

// Returns mapping from job id to error (if present), for all existing jobs
func (server *SubmitServer) ReprioritizeJobs(ctx context.Context, request *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	var jobs []*api.Job
	if len(request.JobIds) > 0 {
		existingJobs, err := server.jobRepository.GetExistingJobsByIds(request.JobIds)
		if err != nil {
			return nil, err
		}
		jobs = existingJobs
	} else if request.Queue != "" && request.JobSetId != "" {
		ids, e := server.jobRepository.GetActiveJobIds(request.Queue, request.JobSetId)
		if e != nil {
			return nil, status.Errorf(codes.Aborted, e.Error())
		}
		existingJobs, e := server.jobRepository.GetExistingJobsByIds(ids)
		if e != nil {
			return nil, status.Errorf(codes.Internal, e.Error())
		}
		jobs = existingJobs
	}

	err := server.checkReprioritizePerms(ctx, jobs)
	if err != nil {
		return nil, err
	}

	principalName := authorization.GetPrincipal(ctx).GetName()

	err = reportJobsReprioritizing(server.eventStore, principalName, jobs, request.NewPriority)
	if err != nil {
		return nil, err
	}

	jobIds := []string{}
	for _, job := range jobs {
		jobIds = append(jobIds, job.Id)
	}
	results, err := server.reprioritizeJobs(jobIds, request.NewPriority, principalName)
	if err != nil {
		return nil, err
	}

	return &api.JobReprioritizeResponse{ReprioritizationResults: results}, nil
}

func (server *SubmitServer) reprioritizeJobs(jobIds []string, newPriority float64, principalName string) (map[string]string, error) {
	updateJobResults, err := server.jobRepository.UpdateJobs(jobIds, func(jobs []*api.Job) {
		for _, job := range jobs {
			job.Priority = newPriority
		}
		err := server.reportReprioritizedJobEvents(jobs, newPriority, principalName)
		if err != nil {
			log.Warnf("Failed to report events for reprioritize of jobs %s: %v", strings.Join(jobIds, ", "), err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("[reprioritizeJobs] error updating jobs: %s", err)
	}

	results := map[string]string{}
	for _, r := range updateJobResults {
		if r.Error == nil {
			results[r.JobId] = ""
		} else {
			results[r.JobId] = r.Error.Error()
		}
	}
	return results, nil
}

func (server *SubmitServer) reportReprioritizedJobEvents(reprioritizedJobs []*api.Job, newPriority float64, principalName string) error {

	err := reportJobsUpdated(server.eventStore, principalName, reprioritizedJobs)
	if err != nil {
		return err
	}

	err = reportJobsReprioritized(server.eventStore, principalName, reprioritizedJobs, newPriority)
	if err != nil {
		return err
	}

	return nil
}

// createJobs returns a list of objects representing the jobs in a JobSubmitRequest.
// This function validates the jobs in the request and the pod specs. in each job.
// If any job or pod in invalid, an error is returned.
func (server *SubmitServer) createJobs(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	if request.JobSetId == "" {
		return nil, fmt.Errorf("[createJobs] job set not specified")
	}

	if request.Queue == "" {
		return nil, fmt.Errorf("[createJobs] queue not specified")
	}

	for i, item := range request.JobRequestItems {

		if item.PodSpec != nil && len(item.PodSpecs) > 0 {
			return nil, fmt.Errorf("[createJobs] jobs must specify either a pod spec. or pod spec. list, but the %d-th job of job set %s specifies both", i, request.JobSetId)
		}

		podSpecs := item.GetAllPodSpecs()
		if len(podSpecs) == 0 {
			return nil, fmt.Errorf("[createJobs] jobs must contain one or more pod specs., but none were found for the %d-th job of job set %s ", i, request.JobSetId)
		}

		if err := validation.ValidateJobSubmitRequestItem(item); err != nil {
			return nil, fmt.Errorf("[createJobs] error validating the %d-th job of job set %s: %w", i, request.JobSetId, err)
		}

		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}

		for j, podSpec := range podSpecs {
			server.applyDefaultsToPodSpec(podSpec)
			err := validation.ValidatePodSpec(podSpec, server.schedulingConfig.MaxPodSpecSizeBytes)
			if err != nil {
				return nil, fmt.Errorf("[createJobs] error validating the %d-th pod pf the %d-th job of job set %s: %w", j, i, request.JobSetId, err)
			}

			// TODO: remove, RequiredNodeLabels is deprecated and will be removed in future versions
			for k, v := range item.RequiredNodeLabels {
				if podSpec.NodeSelector == nil {
					podSpec.NodeSelector = map[string]string{}
				}
				podSpec.NodeSelector[k] = v
			}
		}

		j := &api.Job{
			Id:       util.NewULID(),
			ClientId: item.ClientId,
			Queue:    request.Queue,
			JobSetId: request.JobSetId,

			Namespace:   namespace,
			Labels:      item.Labels,
			Annotations: item.Annotations,

			RequiredNodeLabels: item.RequiredNodeLabels,
			Ingress:            item.Ingress,
			Services:           item.Services,

			Priority: item.Priority,

			PodSpec:                  item.PodSpec,
			PodSpecs:                 item.PodSpecs,
			Created:                  time.Now(),
			Owner:                    owner,
			QueueOwnershipUserGroups: ownershipGroups,
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

func (server *SubmitServer) applyDefaultsToPodSpec(spec *v1.PodSpec) {
	if spec == nil {
		return
	}

	// Add default resource requests and limits if missing
	for i := range spec.Containers {
		c := &spec.Containers[i]
		if c.Resources.Limits == nil {
			c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
		}
		if c.Resources.Requests == nil {
			c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
		}
		for res, val := range server.schedulingConfig.DefaultJobLimits {
			_, hasLimit := c.Resources.Limits[v1.ResourceName(res)]
			_, hasRequest := c.Resources.Limits[v1.ResourceName(res)]

			// TODO Should we check and apply these separately?
			if !hasLimit && !hasRequest {
				c.Resources.Requests[v1.ResourceName(res)] = val
				c.Resources.Limits[v1.ResourceName(res)] = val
			}
		}
	}

	// Each pod must have some default tolerations
	// Here, we add any that are missing
	podTolerations := make(map[string]v1.Toleration)
	for _, podToleration := range spec.Tolerations {
		podTolerations[podToleration.Key] = podToleration
	}
	for _, defaultToleration := range server.schedulingConfig.DefaultJobTolerations {
		podToleration, ok := podTolerations[defaultToleration.Key]
		if !ok || !defaultToleration.MatchToleration(&podToleration) {
			spec.Tolerations = append(spec.Tolerations, defaultToleration)
		}
	}
}

func createJobFailuresWithReason(jobs []*api.Job, reason string) []*jobFailure {
	jobFailures := make([]*jobFailure, len(jobs), len(jobs))
	for i, job := range jobs {
		jobFailures[i] = &jobFailure{
			job:    job,
			reason: reason,
		}
	}
	return jobFailures
}
