package server

import (
	"context"
	"encoding/json"
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
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
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
	q, err := server.queueRepository.GetQueue(req.Name)
	if err == repository.ErrQueueNotFound {
		return nil, status.Errorf(codes.NotFound, "Queue %s does not exist", req.Name)
	}
	if err != nil {
		return nil, err
	}

	principal := authorization.GetPrincipal(ctx)

	fmt.Println(q)
	if !principalHasQueuePermissions(principal, q, queue.PermissionVerbWatch) {
		return nil, status.Errorf(codes.PermissionDenied, "User %s not allowed to perform queue action: %s", principal.GetName(), queue.PermissionVerbWatch)
	}

	jobSets, e := server.jobRepository.GetQueueActiveJobSets(req.Name)
	if e != nil {
		return nil, e
	}

	return &api.QueueInfo{
		Name:          req.Name,
		ActiveJobSets: jobSets,
	}, nil
}

func (server *SubmitServer) GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	queue, e := server.queueRepository.GetQueue(req.Name)
	if e == repository.ErrQueueNotFound {
		return nil, status.Errorf(codes.NotFound, "Queue %q not found", req.Name)

	} else if e != nil {
		return nil, status.Errorf(codes.Unavailable, "Could not load queue %q: %s", req.Name, e.Error())
	}
	return queue.ToAPI(), nil
}

func (server *SubmitServer) CreateQueue(ctx context.Context, request *api.Queue) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.CreateQueue); e != nil {
		return nil, e
	}

	if len(request.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		request.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error: %s", err)
	}

	e := server.queueRepository.CreateQueue(queue)
	if e == repository.ErrQueueAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "Queue %q already exists", request.Name)
	} else if e != nil {
		return nil, status.Errorf(codes.Unavailable, e.Error())
	}
	return &types.Empty{}, nil
}

func (server *SubmitServer) UpdateQueue(ctx context.Context, request *api.Queue) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.CreateQueue); e != nil {
		return nil, e
	}

	queue, err := queue.NewQueue(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	e := server.queueRepository.UpdateQueue(queue)
	if e == repository.ErrQueueNotFound {
		return nil, status.Errorf(codes.NotFound, "Queue %q not found", request.Name)
	} else if e != nil {
		return nil, status.Errorf(codes.Unavailable, e.Error())
	}
	return &types.Empty{}, nil
}

func (server *SubmitServer) DeleteQueue(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.DeleteQueue); e != nil {
		return nil, e
	}

	active, e := server.jobRepository.GetQueueActiveJobSets(request.Name)
	if e != nil {
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}
	if len(active) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "Queue is not empty.")
	}

	e = server.queueRepository.DeleteQueue(request.Name)
	if e != nil {
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}

	return &types.Empty{}, nil
}

func (server *SubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	principal := authorization.GetPrincipal(ctx)

	q, err := server.getQueueOrCreate(ctx, req.Queue)
	if err != nil {
		return nil, err
	}

	if !server.permissions.UserHasPermission(ctx, permissions.SubmitAnyJobs) {
		if !principalHasQueuePermissions(principal, q, queue.PermissionVerbSubmit) {
			return nil, status.Errorf(codes.PermissionDenied, err.Error())
		}
	}

	principalSubject := queue.PermissionSubject{
		Name: principal.GetName(),
		Kind: queue.PermissionSubjectKindUser,
	}

	groups := []string{}
	if !q.HasPermission(principalSubject, queue.PermissionVerbSubmit) {
		for _, subject := range queue.NewPermissionSubjectsFromOwners(nil, principal.GetGroupNames()) {
			if q.HasPermission(subject, queue.PermissionVerbSubmit) {
				groups = append(groups, subject.Name)
			}
		}
	}

	jobs, e := server.createJobs(req, principal.GetName(), groups)
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

func (server *SubmitServer) cancelJobs(ctx context.Context, queueName string, jobs []*api.Job) (*api.CancellationResult, error) {
	principal := authorization.GetPrincipal(ctx)

	q, err := server.queueRepository.GetQueue(queueName)
	if err != nil {
		return nil, err
	}

	if !server.permissions.UserHasPermission(ctx, permissions.CancelAnyJobs) {
		if !principalHasQueuePermissions(principal, q, queue.PermissionVerbCancel) {
			return nil, status.Errorf(codes.PermissionDenied, err.Error())
		}
	}

	e := reportJobsCancelling(server.eventStore, principal.GetName(), jobs)
	if e != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}

	deletionResult := server.jobRepository.DeleteJobs(jobs)
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
	updateJobResults := server.jobRepository.UpdateJobs(jobIds, func(jobs []*api.Job) {
		for _, job := range jobs {
			job.Priority = newPriority
		}
		err := server.reportReprioritizedJobEvents(jobs, newPriority, principalName)
		if err != nil {
			log.Warnf("Failed to report events for reprioritize of jobs %s: %v", strings.Join(jobIds, ", "), err)
		}
	})

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
	queueNames := make(map[string]struct{})
	for _, job := range jobs {
		queueNames[job.Queue] = struct{}{}
	}
	for queueName := range queueNames {
		principal := authorization.GetPrincipal(ctx)

		q, err := server.queueRepository.GetQueue(queueName)
		if err != nil {
			return err
		}

		if !server.permissions.UserHasPermission(ctx, permissions.CancelAnyJobs) {
			if !principalHasQueuePermissions(principal, q, queue.PermissionVerbReprioritize) {
				return status.Errorf(codes.PermissionDenied, err.Error())
			}
		}
	}
	return nil
}

func principalHasQueuePermissions(principal authorization.Principal, q queue.Queue, verb queue.PermissionVerb) bool {
	subjects := queue.PermissionSubjects{}
	for _, group := range principal.GetGroupNames() {
		subjects = append(subjects, queue.PermissionSubject{
			Name: group,
			Kind: queue.PermissionSubjectKindGroup,
		})
	}
	subjects = append(subjects, queue.PermissionSubject{
		Name: principal.GetName(),
		Kind: queue.PermissionSubjectKindUser,
	})

	for _, subject := range subjects {
		if q.HasPermission(subject, verb) {
			return true
		}
	}

	return false
}

func (server *SubmitServer) getQueueOrCreate(ctx context.Context, queueName string) (queue.Queue, error) {
	q, e := server.queueRepository.GetQueue(queueName)
	if e == nil {
		return q, nil
	}

	if e == repository.ErrQueueNotFound {
		if !server.queueManagementConfig.AutoCreateQueues || !server.permissions.UserHasPermission(ctx, permissions.SubmitAnyJobs) {
			return queue.Queue{}, status.Errorf(codes.NotFound, "Queue %q not found", queueName)
		}

		principal := authorization.GetPrincipal(ctx)

		q = queue.Queue{
			Name:           queueName,
			PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
			Permissions: []queue.Permissions{
				queue.NewPermissionsFromOwners([]string{principal.GetName()}, principal.GetGroupNames()),
			},
		}

		if err := server.queueRepository.CreateQueue(q); err != nil {
			return queue.Queue{}, status.Errorf(codes.Aborted, e.Error())
		}
		return q, nil
	}

	return queue.Queue{}, status.Errorf(codes.Unavailable, "Could not load queue %q: %s", queueName, e.Error())
}

func (server *SubmitServer) checkQueuePermissionsWithAutocreate(ctx context.Context,
	queueName string,
	attemptToCreate bool,
	basicPermission permission.Permission,
	allQueuesPermission permission.Permission) ([]string, error) {

	q, e := server.queueRepository.GetQueue(queueName)
	if e == repository.ErrQueueNotFound {
		if attemptToCreate &&
			server.queueManagementConfig.AutoCreateQueues &&
			server.permissions.UserHasPermission(ctx, permissions.SubmitAnyJobs) {
			q = queue.Queue{
				Name:           queueName,
				PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
			}
			e := server.queueRepository.CreateQueue(q)
			if e != nil {
				return nil, status.Errorf(codes.Aborted, e.Error())
			}
			return nil, nil
		} else {
			return nil, status.Errorf(codes.NotFound, "Queue %q not found", queueName)
		}
	} else if e != nil {
		return nil, status.Errorf(codes.Unavailable, "Could not load queue %q: %s", queueName, e.Error())
	}

	permissionToCheck := basicPermission
	owned, groups := server.permissions.UserOwns(ctx, q.ToAPI())
	if !owned {
		permissionToCheck = allQueuesPermission
	}
	if e := checkPermission(server.permissions, ctx, permissionToCheck); e != nil {
		return nil, e
	}
	return groups, nil
}

func (server *SubmitServer) createJobs(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	if request.JobSetId == "" {
		return nil, fmt.Errorf("job set is not specified")
	}

	if request.Queue == "" {
		return nil, fmt.Errorf("queue is not specified")
	}

	for i, item := range request.JobRequestItems {

		if item.PodSpec != nil && len(item.PodSpecs) > 0 {
			return nil, fmt.Errorf("job with index %v has both pod spec and pod spec list specified", i)
		}

		if len(item.GetAllPodSpecs()) == 0 {
			return nil, fmt.Errorf("job with index %v has no pod spec", i)
		}

		e := validation.ValidateJobSubmitRequestItem(item)
		if e != nil {
			return nil, fmt.Errorf("job with index %v: %v", i, e)
		}

		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}

		for j, podSpec := range item.GetAllPodSpecs() {
			server.applyDefaultsToPodSpec(podSpec)
			e := validation.ValidatePodSpec(podSpec, server.schedulingConfig.MaxPodSpecSizeBytes)
			if e != nil {
				return nil, fmt.Errorf("error validating pod spec of job with index %v, pod: %v: %v", i, j, e)
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
	if spec != nil {
		for i := range spec.Containers {
			c := &spec.Containers[i]
			if c.Resources.Limits == nil {
				c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
			}
			if c.Resources.Requests == nil {
				c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
			}
			for k, v := range server.schedulingConfig.DefaultJobLimits {
				_, limitExists := c.Resources.Limits[v1.ResourceName(k)]
				_, requestExists := c.Resources.Limits[v1.ResourceName(k)]
				if !limitExists && !requestExists {
					c.Resources.Requests[v1.ResourceName(k)] = v
					c.Resources.Limits[v1.ResourceName(k)] = v
				}
			}
		}
		tolerationsToAdd := []v1.Toleration{}
		for _, defaultToleration := range server.schedulingConfig.DefaultJobTolerations {
			exists := false
			for _, existingToleration := range spec.Tolerations {
				if defaultToleration.MatchToleration(&existingToleration) {
					exists = true
					break
				}
			}
			if !exists {
				tolerationsToAdd = append(tolerationsToAdd, defaultToleration)
			}
		}
		spec.Tolerations = append(spec.Tolerations, tolerationsToAdd...)
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
