package server

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/pkg/api"
)

type SubmitServer struct {
	permissions              authorization.PermissionChecker
	jobRepository            repository.JobRepository
	queueRepository          repository.QueueRepository
	eventStore               repository.EventStore
	schedulingInfoRepository repository.SchedulingInfoRepository
	queueManagementConfig    *configuration.QueueManagementConfig
}

func NewSubmitServer(
	permissions authorization.PermissionChecker,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	queueManagementConfig *configuration.QueueManagementConfig) *SubmitServer {

	return &SubmitServer{
		permissions:              permissions,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository,
		queueManagementConfig:    queueManagementConfig}
}

func (server *SubmitServer) GetQueueInfo(ctx context.Context, req *api.QueueInfoRequest) (*api.QueueInfo, error) {
	if e := checkPermission(server.permissions, ctx, permissions.WatchAllEvents); e != nil {
		return nil, e
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
	return queue, nil
}

func (server *SubmitServer) CreateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.CreateQueue); e != nil {
		return nil, e
	}

	if len(queue.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		queue.UserOwners = []string{principal.GetName()}
	}

	e := validateQueue(queue)
	if e != nil {
		return nil, e
	}

	e = server.queueRepository.CreateQueue(queue)
	if e == repository.ErrQueueAlreadyExists {
		return nil, status.Errorf(codes.AlreadyExists, "Queue %q already exists", queue.Name)
	} else if e != nil {
		return nil, status.Errorf(codes.Unavailable, e.Error())
	}
	return &types.Empty{}, nil
}

func (server *SubmitServer) UpdateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.CreateQueue); e != nil {
		return nil, e
	}

	e := validateQueue(queue)
	if e != nil {
		return nil, e
	}

	e = server.queueRepository.UpdateQueue(queue)
	if e == repository.ErrQueueNotFound {
		return nil, status.Errorf(codes.NotFound, "Queue %q not found", queue.Name)
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
	e, ownershipGroups := server.checkQueuePermission(ctx, req.Queue, true, permissions.SubmitJobs, permissions.SubmitAnyJobs)
	if e != nil {
		return nil, e
	}

	principal := authorization.GetPrincipal(ctx)

	jobs, e := server.jobRepository.CreateJobs(req, principal.GetName(), ownershipGroups)
	if e != nil {
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}

	e = server.validateJobsCanBeScheduled(jobs)
	if e != nil {
		return nil, status.Errorf(codes.InvalidArgument, e.Error())
	}

	e = reportSubmitted(server.eventStore, jobs)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}

	submissionResults, e := server.jobRepository.AddJobs(jobs)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}

	result := &api.JobSubmitResponse{
		JobResponseItems: make([]*api.JobSubmitResponseItem, 0, len(submissionResults)),
	}

	createdJobs := []*api.Job{}
	doubleSubmits := []*repository.SubmitJobResult{}
	for i, submissionResult := range submissionResults {
		jobResponse := &api.JobSubmitResponseItem{JobId: submissionResult.JobId}
		if submissionResult.Error != nil {
			jobResponse.Error = submissionResult.Error.Error()
		}
		result.JobResponseItems = append(result.JobResponseItems, jobResponse)

		if submissionResult.Error == nil {
			if submissionResult.DuplicateDetected {
				doubleSubmits = append(doubleSubmits, submissionResult)
			} else {
				createdJobs = append(createdJobs, jobs[i])
			}
		}
	}

	e = reportDuplicateDetected(server.eventStore, doubleSubmits)
	if e != nil {
		return result, status.Errorf(codes.Internal, e.Error())
	}

	e = reportQueued(server.eventStore, createdJobs)
	if e != nil {
		return result, status.Errorf(codes.Internal, e.Error())
	}
	return result, nil
}

func (server *SubmitServer) validateJobsCanBeScheduled(jobs []*api.Job) error {
	allClusterSchedulingInfo, e := server.schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		return e
	}

	activeClusterSchedulingInfo := scheduling.FilterActiveClusterSchedulingInfoReports(allClusterSchedulingInfo)
	for i, job := range jobs {
		if !scheduling.MatchSchedulingRequirementsOnAnyCluster(job, activeClusterSchedulingInfo) {
			return fmt.Errorf("job with index %d is not schedulable on any cluster", i)
		}
	}

	return nil
}

func (server *SubmitServer) CancelJobs(ctx context.Context, request *api.JobCancelRequest) (*api.CancellationResult, error) {
	if request.JobId != "" {
		jobs, e := server.jobRepository.GetExistingJobsByIds([]string{request.JobId})
		if e != nil {
			return nil, status.Errorf(codes.Internal, e.Error())
		}
		return server.cancelJobs(ctx, jobs[0].Queue, jobs)
	}

	if request.JobSetId != "" && request.Queue != "" {
		ids, e := server.jobRepository.GetActiveJobIds(request.Queue, request.JobSetId)
		if e != nil {
			return nil, status.Errorf(codes.Aborted, e.Error())
		}
		jobs, e := server.jobRepository.GetExistingJobsByIds(ids)
		if e != nil {
			return nil, status.Errorf(codes.Internal, e.Error())
		}
		return server.cancelJobs(ctx, request.Queue, jobs)
	}
	return nil, status.Errorf(codes.InvalidArgument, "Specify job id or queue with job set id")
}

func (server *SubmitServer) cancelJobs(ctx context.Context, queue string, jobs []*api.Job) (*api.CancellationResult, error) {
	if e, _ := server.checkQueuePermission(ctx, queue, false, permissions.CancelJobs, permissions.CancelAnyJobs); e != nil {
		return nil, e
	}
	principal := authorization.GetPrincipal(ctx)

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

	results, err := server.reprioritizeJobs(authorization.GetPrincipal(ctx), jobs, request)
	if err != nil {
		return nil, err
	}

	return &api.JobReprioritizeResponse{ReprioritizationResults: results}, nil
}

func (server *SubmitServer) reprioritizeJobs(principal authorization.Principal, jobs []*api.Job, request *api.JobReprioritizeRequest) (map[string]string, error) {
	err := reportJobsReprioritizing(server.eventStore, principal.GetName(), jobs, request.NewPriority)
	if err != nil {
		return nil, err
	}

	results, err := server.jobRepository.UpdatePriority(jobs, request.NewPriority)
	if err != nil {
		return nil, err
	}

	jobMap := make(map[string]*api.Job)
	for _, job := range jobs {
		jobMap[job.Id] = job
	}

	var reprioritizedJobs []*api.Job
	for jobId, errorString := range results {
		if errorString == "" {
			reprioritizedJobs = append(reprioritizedJobs, jobMap[jobId])
		}
	}

	err = reportJobsReprioritized(server.eventStore, principal.GetName(), reprioritizedJobs, request.NewPriority)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (server *SubmitServer) checkReprioritizePerms(ctx context.Context, jobs []*api.Job) error {
	queues := make(map[string]bool)
	for _, job := range jobs {
		queues[job.Queue] = true
	}
	for queue, _ := range queues {
		if e, _ := server.checkQueuePermission(ctx, queue, false, permissions.ReprioritizeJobs, permissions.ReprioritizeAnyJobs); e != nil {
			return e
		}
	}
	return nil
}

func (server *SubmitServer) checkQueuePermission(
	ctx context.Context,
	queueName string,
	attemptToCreate bool,
	basicPermission permission.Permission,
	allQueuesPermission permission.Permission) (e error, ownershipGroups []string) {

	queue, e := server.queueRepository.GetQueue(queueName)
	if e == repository.ErrQueueNotFound {
		if attemptToCreate &&
			server.queueManagementConfig.AutoCreateQueues &&
			server.permissions.UserHasPermission(ctx, permissions.SubmitAnyJobs) {

			queue = &api.Queue{
				Name:           queueName,
				PriorityFactor: server.queueManagementConfig.DefaultPriorityFactor,
			}
			e := server.queueRepository.CreateQueue(queue)
			if e != nil {
				return status.Errorf(codes.Aborted, e.Error()), []string{}
			}
			return nil, []string{}
		} else {
			return status.Errorf(codes.NotFound, "Queue %q not found", queueName), []string{}
		}
	} else if e != nil {
		return status.Errorf(codes.Unavailable, "Could not load queue %q: %s", queueName, e.Error()), []string{}
	}

	permissionToCheck := basicPermission
	owned, groups := server.permissions.UserOwns(ctx, queue)
	if !owned {
		permissionToCheck = allQueuesPermission
	}
	if e := checkPermission(server.permissions, ctx, permissionToCheck); e != nil {
		return e, []string{}
	}
	return nil, groups
}

func validateQueue(queue *api.Queue) error {
	if queue.PriorityFactor < 1.0 {
		return status.Errorf(codes.InvalidArgument, "Minimum queue priority factor is 1.")
	}
	return nil
}
