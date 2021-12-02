package server

import (
	"context"
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
	queueManagementConfig    *configuration.QueueManagementConfig
	schedulingConfig         *configuration.SchedulingConfig
}

func NewSubmitServer(
	permissions authorization.PermissionChecker,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	queueManagementConfig *configuration.QueueManagementConfig,
	schedulingConfig *configuration.SchedulingConfig,
) *SubmitServer {

	return &SubmitServer{
		permissions:              permissions,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository,
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
	handler := handlers.SubmitJobs(
		NewJobs(
			authorization.GetPrincipal(ctx).GetName(),
			GetQueueOwnership(
				repository.GetQueueFn(server.queueRepository.GetQueue).
					Autocreate(
						server.queueManagementConfig.AutoCreateQueues,
						server.queueManagementConfig.DefaultPriorityFactor,
						server.queueRepository.CreateQueue,
					),
				server.permissions.UserOwns,
			),
			api.JobsFromSubmitRequest(server.applyDefaultsToPodSpec, util.NewULID, time.Now),
		).Validate(
			server.schedulingInfoRepository.GetClusterSchedulingInfo,
			validateJobsCanBeScheduled,
		),
		repository.AddJobs(server.jobRepository.AddJobs).
			ReportDuplicate(server.eventStore.ReportEvents).
			ReportSubmitted(server.eventStore.ReportEvents).
			ReportQueued(server.eventStore.ReportEvents),
	).Validate(
		validation.JobSubmitRequestItem(server.schedulingConfig.MaxPodSpecSizeBytes),
	).Authorize(
		server.authorizeOwnership,
		server.permissions.UserHasPermission,
		server.queueManagementConfig.AutoCreateQueues,
	)

	return handler(ctx, req)
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
	queues := make(map[string]bool)
	for _, job := range jobs {
		queues[job.Queue] = true
	}
	for queue := range queues {
		if e, _ := server.checkQueuePermission(ctx, queue, false, permissions.ReprioritizeJobs, permissions.ReprioritizeAnyJobs); e != nil {
			return e
		}
	}
	return nil
}

func (server *SubmitServer) authorizeOwnership(ctx context.Context, queueName string) (bool, error) {
	queue, err := server.queueRepository.GetQueue(queueName)
	if err != nil {
		return false, err
	}

	owns, _ := server.permissions.UserOwns(ctx, queue)

	return owns, nil
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
