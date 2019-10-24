package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
)

type SubmitServer struct {
	permissions     authorization.PermissionChecker
	jobRepository   repository.JobRepository
	queueRepository repository.QueueRepository
	eventRepository repository.EventRepository
}

func NewSubmitServer(
	permissions authorization.PermissionChecker,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	eventRepository repository.EventRepository) *SubmitServer {

	return &SubmitServer{
		permissions:     permissions,
		jobRepository:   jobRepository,
		queueRepository: queueRepository,
		eventRepository: eventRepository}
}

func (server *SubmitServer) CreateQueue(ctx context.Context, queue *api.Queue) (*types.Empty, error) {
	if e := checkPermission(server.permissions, ctx, permissions.CreateQueue); e != nil {
		return nil, e
	}

	if len(queue.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		queue.UserOwners = []string{principal.GetName()}
	}

	e := server.queueRepository.CreateQueue(queue)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}
	return &types.Empty{}, nil
}

func (server *SubmitServer) SubmitJob(ctx context.Context, req *api.JobRequest) (*api.JobSubmitResponse, error) {
	if e := server.checkQueuePermission(ctx, req.Queue, permissions.SubmitJobs, permissions.SubmitAnyJobs); e != nil {
		return nil, e
	}

	job := server.jobRepository.CreateJob(req)

	e := reportSubmitted(server.eventRepository, job)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}

	e = server.jobRepository.AddJob(job)
	if e != nil {
		return nil, status.Errorf(codes.Aborted, e.Error())
	}
	result := &api.JobSubmitResponse{JobId: job.Id}

	e = reportQueued(server.eventRepository, job)
	if e != nil {
		return result, status.Errorf(codes.Aborted, e.Error())
	}

	return result, nil
}

func (server *SubmitServer) CancelJobs(ctx context.Context, request *api.JobCancelRequest) (*api.CancellationResult, error) {
	if request.JobId != "" {
		jobs, e := server.jobRepository.GetJobsByIds([]string{request.JobId})
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
		jobs, e := server.jobRepository.GetJobsByIds(ids)
		if e != nil {
			return nil, status.Errorf(codes.Internal, e.Error())
		}
		return server.cancelJobs(ctx, request.Queue, jobs)
	}
	return nil, status.Errorf(codes.InvalidArgument, "Specify job id or queue with job set id")
}

func (server *SubmitServer) cancelJobs(ctx context.Context, queue string, jobs []*api.Job) (*api.CancellationResult, error) {
	if e := server.checkQueuePermission(ctx, queue, permissions.CancelJobs, permissions.CancelAnyJobs); e != nil {
		return nil, e
	}

	e := reportJobsCancelling(server.eventRepository, jobs)
	if e != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}

	cancellationResult := server.jobRepository.Cancel(jobs)
	cancelled := []*api.Job{}
	cancelledIds := []string{}
	for job, error := range cancellationResult {
		if error != nil {
			log.Errorf("Error when cancelling job id %s: %s", job.Id, error.Error())
		} else {
			cancelled = append(cancelled, job)
			cancelledIds = append(cancelledIds, job.Id)
		}
	}

	e = reportJobsCancelled(server.eventRepository, cancelled)
	if e != nil {
		return nil, status.Errorf(codes.Unknown, e.Error())
	}

	return &api.CancellationResult{cancelledIds}, nil
}

func (server *SubmitServer) checkQueuePermission(
	ctx context.Context,
	queueName string,
	basicPermission permissions.Permission,
	allQueuesPermission permissions.Permission) error {

	queue, e := server.queueRepository.GetQueue(queueName)
	if e != nil {
		return status.Errorf(codes.NotFound, "Could not load queue: %s", e.Error())
	}
	permissionToCheck := basicPermission
	if !server.permissions.UserOwns(ctx, queue) {
		permissionToCheck = allQueuesPermission
	}
	if e := checkPermission(server.permissions, ctx, permissionToCheck); e != nil {
		return e
	}
	return nil
}
