package server

import (
	"context"
	"encoding/json"
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
	"github.com/G-Research/armada/internal/armada/server/handlers"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
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
	handler := handlers.GetQueueInfo(server.jobRepository.GetQueueActiveJobSets).
		Authorize(server.permissions.UserHasPermission, permissions.WatchAllEvents)

	return handler(ctx, req)
}

func (server *SubmitServer) GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	handler := handlers.GetQueue(server.queueRepository.GetQueue)

	return handler(ctx, req)
}

func (server *SubmitServer) CreateQueue(ctx context.Context, req *api.Queue) (*types.Empty, error) {
	handler := handlers.CreateQueue(server.queueRepository.CreateQueue).
		Ownership(authorization.GetPrincipal(ctx).GetName()).
		Validate().
		Authorize(server.permissions.UserHasPermission, permissions.CreateQueue)

	return handler(ctx, req)
}

func (server *SubmitServer) UpdateQueue(ctx context.Context, req *api.Queue) (*types.Empty, error) {
	handler := handlers.UpdateQueue(server.queueRepository.UpdateQueue).
		Validate().
		Authorize(server.permissions.UserHasPermission, permissions.CreateQueue)

	return handler(ctx, req)
}

func (server *SubmitServer) DeleteQueue(ctx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	handler := handlers.DeleteQueue(server.queueRepository.DeleteQueue).
		CheckIfEmpty(server.jobRepository.GetQueueActiveJobSets).
		Authorize(server.permissions.UserHasPermission, permissions.DeleteQueue)

	return handler(ctx, req)
}

func (server *SubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ownershipGroups, e := server.checkQueuePermission(ctx, req.Queue, true, permissions.SubmitJobs, permissions.SubmitAnyJobs)
	if e != nil {
		return nil, e
	}

	principal := authorization.GetPrincipal(ctx)

	jobs, e := server.createJobs(req, principal.GetName(), ownershipGroups)
	if e != nil {
		reqJson, _ := json.Marshal(req)
		log.Errorf("Error submitting job %s for user %s: %v", reqJson, principal.GetName(), e)
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}

	allClusterSchedulingInfo, e := server.schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		return nil, e
	}

	e = validateJobsCanBeScheduled(jobs, allClusterSchedulingInfo)
	if e != nil {
		reqJson, _ := json.Marshal(req)
		log.Errorf("Error submitting job %s for user %s: %v", reqJson, principal.GetName(), e)
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}

	e = reportSubmitted(server.eventStore, jobs)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}

	submissionResults, e := server.jobRepository.AddJobs(jobs)
	if e != nil {
		jobFailures := createJobFailuresWithReason(jobs, fmt.Sprintf("Failed to save job in Armada: %v", e))
		reportErr := reportFailed(server.eventStore, "", jobFailures)
		if reportErr != nil {
			return nil, status.Errorf(codes.Internal, "error when reporting failure event: %v", reportErr)
		}
		return nil, status.Errorf(codes.Aborted, e.Error())
	}

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

	e = reportFailed(server.eventStore, "", jobFailures)
	if e != nil {
		return result, status.Errorf(
			codes.Internal, fmt.Sprintf("Failed to report failed events for jobs: %v", e))
	}

	e = reportDuplicateDetected(server.eventStore, doubleSubmits)
	if e != nil {
		return result, status.Errorf(
			codes.Internal, fmt.Sprintf("Failed to report job duplicate submission events for jobs: %v", e))
	}

	e = reportQueued(server.eventStore, createdJobs)
	if e != nil {
		return result, status.Errorf(
			codes.Internal, fmt.Sprintf("Failed to report queued events for jobs: %v", e))
	}

	if len(jobFailures) > 0 {
		return result, status.Errorf(
			codes.Unavailable, fmt.Sprintf("Some jobs failed to be submitted"))
	}

	return result, nil
}

func (server *SubmitServer) CancelJobs(ctx context.Context, request *api.JobCancelRequest) (*api.CancellationResult, error) {
	if request.JobId != "" {
		jobs, err := server.jobRepository.GetExistingJobsByIds([]string{request.JobId})
		if err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		return server.cancelJobs(ctx, jobs[0].Queue, jobs)
	}

	if request.JobSetId != "" && request.Queue != "" {
		ids, err := server.jobRepository.GetActiveJobIds(request.Queue, request.JobSetId)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, err.Error())
		}

		batches := util.Batch(ids, server.cancelJobsBatchSize)
		cancelledIds := []string{}
		for _, batch := range batches {
			jobs, err := server.jobRepository.GetExistingJobsByIds(batch)
			if err != nil {
				return &api.CancellationResult{CancelledIds: cancelledIds}, status.Errorf(
					codes.Internal,
					"failed to find some jobs: %v", err)
			}
			result, err := server.cancelJobs(ctx, request.Queue, jobs)
			if err != nil {
				return &api.CancellationResult{CancelledIds: cancelledIds}, status.Errorf(
					codes.Internal,
					"failed to cancel some jobs: %v", err)
			}
			cancelledIds = append(cancelledIds, result.CancelledIds...)

			if util.CloseToDeadline(ctx, time.Second*1) {
				return &api.CancellationResult{CancelledIds: cancelledIds}, status.Errorf(
					codes.DeadlineExceeded,
					"not all jobs were cancelled: took too long")
			}
		}
		return &api.CancellationResult{CancelledIds: cancelledIds}, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "Specify job id or queue with job set id")
}

func (server *SubmitServer) cancelJobs(ctx context.Context, queue string, jobs []*api.Job) (*api.CancellationResult, error) {
	if _, err := server.checkQueuePermission(ctx, queue, false, permissions.CancelJobs, permissions.CancelAnyJobs); err != nil {
		return nil, err
	}
	principal := authorization.GetPrincipal(ctx)

	e := reportJobsCancelling(server.eventStore, principal.GetName(), jobs)
	if e != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}

	deletionResult, err := server.jobRepository.DeleteJobs(jobs)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}
	cancelled := []*api.Job{}
	cancelledIds := []string{}
	for job, err := range deletionResult {
		if err != nil {
			log.Errorf("Error when cancelling job id %s: %s", job.Id, err.Error())
		} else {
			cancelled = append(cancelled, job)
			cancelledIds = append(cancelledIds, job.Id)
		}
	}

	e = reportJobsCancelled(server.eventStore, principal.GetName(), cancelled)
	if e != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}

	return &api.CancellationResult{cancelledIds}, nil
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
		return nil, fmt.Errorf("[SubmitServer.reprioritizeJobs] error updating jobs: %s", err)
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

func (server *SubmitServer) checkReprioritizePerms(ctx context.Context, jobs []*api.Job) error {
	queues := make(map[string]bool)
	for _, job := range jobs {
		queues[job.Queue] = true
	}
	for queue := range queues {
		if _, err := server.checkQueuePermission(ctx, queue, false, permissions.ReprioritizeJobs, permissions.ReprioritizeAnyJobs); err != nil {
			return err
		}
	}
	return nil
}

// checkQueuePermission checks if the principal embedded in the context has permission
// to perform actions on the given queue. If the principal has sufficient permissions,
// nil is returned. Otherwise an error is returned.
//
// TODO We should change the order of the return values.
// The convention is for the error to be the last of the return values.
func (server *SubmitServer) checkQueuePermission(
	ctx context.Context,
	queueName string,
	attemptToCreate bool,
	basicPermission permission.Permission,
	allQueuesPermission permission.Permission) ([]string, error) {

	// Load the queue into memory to check if the user is the owner of the queue
	queue, err := server.queueRepository.GetQueue(queueName)
	var e *repository.ErrQueueNotFound

	// TODO Checking permissions shouldn't have side side effects.
	// Hence, this function shouldn't automatically create queues.
	// Further, we should consider removing the AutoCreateQueues option entirely.
	// Since it leads to surprising behavior (e.g., creating a queue if the name is misspelled).
	// Creating queues should always be an explicit decision.
	// The less surprising behavior is to return ErrQueueNotFound (perhaps wrapped).
	if errors.As(err, &e) && attemptToCreate && server.queueManagementConfig.AutoCreateQueues {
		// TODO Is this correct? Shouldn't the relevant permission be permissions.CreateQueue?
		err = checkPermission(server.permissions, ctx, permissions.SubmitAnyJobs)
		if err != nil {
			return nil, fmt.Errorf("[checkQueuePermission] error: %w", err)
		}

		queue = &api.Queue{
			Name:           queueName,
			PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
		}
		err = server.queueRepository.CreateQueue(queue)
		if err != nil {
			return nil, fmt.Errorf("[checkQueuePermission] error creating queue: %w", err)
		}

		// nil indicates that the user has sufficient permissions
		// The newly created group has no ownership groups
		return []string{}, nil
	} else if err != nil {
		return nil, fmt.Errorf("[checkQueuePermission] error getting queue %s: %w", queueName, err)
	}

	// The user must either own the queue or have permission to access all queues
	//
	// TODO We should have a more specific permission denied error that includes
	// the resource involved (e.g., that it's a queue and the name of the queue),
	// the action attempted (e.g., submitting a job), the the principal (user) name,
	// and the permission required for the action.
	permissionToCheck := basicPermission
	owned, groups := server.permissions.UserOwns(ctx, queue)
	if !owned {
		permissionToCheck = allQueuesPermission
	}
	if err := checkPermission(server.permissions, ctx, permissionToCheck); err != nil {
		return nil, fmt.Errorf("[checkQueuePermission] permission error for queue %s: %w", queueName, err)
	}
	return groups, nil
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
