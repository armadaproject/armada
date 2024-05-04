package submit

import (
	"context"
	"fmt"
	"math"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/armada/submit/conversion"
	"github.com/armadaproject/armada/internal/armada/submit/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// Server is a service that accepts API calls according to the original Armada submit API and publishes messages
// to Pulsar based on those calls.
type Server struct {
	publisher             pulsarutils.Publisher
	queueRepository       repository.QueueRepository
	queueCache            repository.ReadOnlyQueueRepository
	jobRepository         repository.JobRepository
	submissionConfig      configuration.SubmissionConfig
	deduplicator          Deduplicator
	authorizer            server.ActionAuthorizer
	requireQueueAndJobSet bool
	// Below are used only for testing
	clock       clock.Clock
	idGenerator func() *armadaevents.Uuid
}

func NewServer(
	publisher pulsarutils.Publisher,
	queueRepository repository.QueueRepository,
	queueCache repository.ReadOnlyQueueRepository,
	jobRepository repository.JobRepository,
	submissionConfig configuration.SubmissionConfig,
	deduplicator Deduplicator,
	authorizer server.ActionAuthorizer,
	requireQueueAndJobSet bool,
) *Server {
	return &Server{
		publisher:             publisher,
		queueRepository:       queueRepository,
		queueCache:            queueCache,
		jobRepository:         jobRepository,
		submissionConfig:      submissionConfig,
		deduplicator:          deduplicator,
		authorizer:            authorizer,
		requireQueueAndJobSet: requireQueueAndJobSet,
		clock:                 clock.RealClock{},
		idGenerator: func() *armadaevents.Uuid {
			return armadaevents.MustProtoUuidFromUlidString(util.NewULID())
		},
	}
}

// SubmitJobs allows users to submit jobs to Armada.  On receipt of a request, the following actions are performed:
//   - The request is validated to make sure it is well formed.  If this fails then an error is returned
//   - Each JobRequestItem inside the request is checked to see if it is a duplicate
//   - Each non-duplicate is converted into an armadaevents.SubmitMessage
//   - All SubmitMessages are checked to see if the job they define can be scheduled (an example of a job that cannot
//     be scheduled would be a job that requires more resources than exists on any node).  If any message fails this
//     check then an error is returned.
//   - The SubmitMessages are published to Pulsar.
func (s *Server) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	// Check that the user is actually allowed to submit jobs
	userId, groups, err := s.authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// Validate the request is well-formed
	if err = validation.ValidateSubmitRequest(req, s.submissionConfig); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Get a mapping between req.ClientId and existing jobId.  If such a mapping exists, it means that
	// this job has already been submitted.
	originalIds, err := s.deduplicator.GetOriginalJobIds(ctx, req.Queue, req.JobRequestItems)
	if err != nil {
		// Deduplication is best-effort, therefore this is not fatal
		log.WithError(err).Warn("Error fetching original job ids, deduplication will not occur.")
	}

	submitMsgs := make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems))
	jobResponses := make([]*api.JobSubmitResponseItem, 0, len(req.JobRequestItems))
	idMappings := make(map[string]string, len(req.JobRequestItems))

	for _, jobRequest := range req.JobRequestItems {

		// Check if this job has already been submitted. If so we can simply return the previously submitted id
		originalId, isDuplicate := originalIds[jobRequest.ClientId]
		if isDuplicate {
			ctx.Infof("Job with client id %s is a duplicate of %s", jobRequest.ClientId, originalId)
			jobResponses = append(jobResponses, &api.JobSubmitResponseItem{JobId: originalId})
			continue
		}

		// If we get to here then it isn't a duplicate. Create a Job submission and a job response
		submitMsg := conversion.SubmitJobFromApiRequest(jobRequest, s.submissionConfig, req.JobSetId, req.Queue, userId, s.idGenerator)
		eventTime := s.clock.Now().UTC()
		submitMsgs = append(submitMsgs, &armadaevents.EventSequence_Event{
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: submitMsg,
			},
		})

		jobResponses = append(jobResponses, &api.JobSubmitResponseItem{
			JobId: armadaevents.MustUlidStringFromProtoUuid(submitMsg.JobId),
		})

		if jobRequest.ClientId != "" {
			idMappings[jobRequest.ClientId] = armadaevents.MustUlidStringFromProtoUuid(submitMsg.JobId)
		}
	}

	// If we have no submit msgs then we can return early
	if len(submitMsgs) == 0 {
		return &api.JobSubmitResponse{JobResponseItems: jobResponses}, nil
	}

	// Check if all jobs can be scheduled.
	es := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     submitMsgs,
	}

	pulsarJobDetails := armadaslices.Map(
		jobResponses,
		func(r *api.JobSubmitResponseItem) *schedulerobjects.PulsarSchedulerJobDetails {
			return &schedulerobjects.PulsarSchedulerJobDetails{
				JobId:  r.JobId,
				Queue:  req.Queue,
				JobSet: req.JobSetId,
			}
		})

	if err = s.jobRepository.StorePulsarSchedulerJobDetails(ctx, pulsarJobDetails); err != nil {
		log.WithError(err).Error("failed store pulsar job details")
		return nil, status.Error(codes.Internal, "failed store pulsar job details")
	}

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		log.WithError(err).Error("failed send events to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
	}

	// Store the deduplication ids. Note that this will not be called if pulsar submission has failed, hence
	// a partial pulsar submission can result in duplicate jobs.
	if err = s.deduplicator.StoreOriginalJobIds(ctx, req.Queue, idMappings); err != nil {
		log.WithError(err).Warn("failed to store deduplication ids")
	}
	return &api.JobSubmitResponse{JobResponseItems: jobResponses}, nil
}

func (s *Server) CancelJobs(grpcCtx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.JobSetId == "" || req.Queue == "" {
		ctx.
			WithField("apidatamissing", "true").
			Warnf("Cancel jobs called with missing data: jobId=%s, jobset=%s, queue=%s, user=%s", req.JobId, req.JobSetId, req.Queue, s.GetUser(ctx))
	}

	// separate code path for multiple jobs
	if len(req.JobIds) > 0 {
		return s.cancelJobsByIdsQueueJobset(ctx, req.JobIds, req.Queue, req.JobSetId, req.Reason)
	}

	// Another separate code path for cancelling an entire job set
	// TODO: We should deprecate this and move people over to CancelJobSet()
	if req.JobId == "" {
		log.Warnf("CancelJobs called for queue=%s and jobset=%s but with empty job id. Redirecting to CancelJobSet()", req.Queue, req.JobSetId)
		_, err := s.CancelJobSet(ctx, &api.JobSetCancelRequest{
			Queue:    req.Queue,
			JobSetId: req.JobSetId,
			Reason:   req.Reason,
		})
		if err != nil {
			return nil, err
		}
		return &api.CancellationResult{
			CancelledIds: []string{req.JobId}, // we return an empty string here which seems a bit nonsensical- but that's what the old code did!
		}, nil
	}

	resolvedQueue := ""
	resolvedJobSet := ""

	if s.requireQueueAndJobSet {
		err := validation.ValidateQueueAndJobSet(req)
		if err != nil {
			return nil, err
		}
		resolvedQueue = req.Queue
		resolvedJobSet = req.JobSetId
	} else {
		// resolve the queue and jobset of the job: we can't trust what the user has given us
		resolvedQueue, resolvedJobSet, err := s.resolveQueueAndJobsetForJob(ctx, req.JobId)
		if err != nil {
			return nil, err
		}

		// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
		// since the job could not be found for the provided queue/jobSetId.
		if req.Queue != "" && req.Queue != resolvedQueue {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   req.JobId,
				Message: fmt.Sprintf("job not found in queue %s, try waiting", req.Queue),
			}
		}
		if req.JobSetId != "" && req.JobSetId != resolvedJobSet {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   req.JobId,
				Message: fmt.Sprintf("job not found in job set %s, try waiting", req.JobSetId),
			}
		}
	}

	userId, groups, err := s.authorize(ctx, resolvedQueue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
	if err != nil {
		return nil, err
	}

	sequence := &armadaevents.EventSequence{
		Queue:      resolvedQueue,
		JobSetName: resolvedJobSet,
		UserId:     userId,
		Groups:     groups,
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: pointer.Now(),
				Event: &armadaevents.EventSequence_Event_CancelJob{
					CancelJob: &armadaevents.CancelJob{
						JobId:  jobId,
						Reason: util.Truncate(req.Reason, 512),
					},
				},
			},
		},
	}

	// we can send the message to cancel to both schedulers. If the scheduler it doesn't belong to it'll be a no-op
	err = s.publisher.PublishMessages(ctx, sequence)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: []string{req.JobId}, // indicates no error
	}, nil
}

func (s *Server) PreemptJobs(grpcCtx context.Context, req *api.JobPreemptRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue cannot be empty when preempting jobs",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "jobset cannot be empty when preempting jobs",
		}
	}

	userId, groups, err := s.authorize(ctx, req.Queue, permissions.PreemptAnyJobs, queue.PermissionVerbPreempt)
	if err != nil {
		return nil, err
	}

	sequence, err := preemptJobEventSequenceForJobIds(req.JobIds, req.Queue, req.JobSetId, userId, groups)
	if err != nil {
		return nil, err
	}

	// send the message to both schedulers because jobs may be on either
	err = s.publisher.PublishMessages(ctx, sequence)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &types.Empty{}, nil
}

func preemptJobEventSequenceForJobIds(jobIds []string, q, jobSet, userId string, groups []string) (*armadaevents.EventSequence, error) {
	sequence := &armadaevents.EventSequence{
		Queue:      q,
		JobSetName: jobSet,
		UserId:     userId,
		Groups:     groups,
		Events:     []*armadaevents.EventSequence_Event{},
	}
	for _, jobIdStr := range jobIds {
		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
		if err != nil {
			log.WithError(err).Errorf("could not convert job id to uuid: %s", jobIdStr)
			return nil, fmt.Errorf("could not convert job id to uuid: %s", jobIdStr)
		}
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: pointer.Now(),
			Event: &armadaevents.EventSequence_Event_JobPreemptionRequested{
				JobPreemptionRequested: &armadaevents.JobPreemptionRequested{
					JobId: jobId,
				},
			},
		})
	}
	return sequence, nil
}

func (s *Server) ReprioritizeJobs(grpcCtx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if s.requireQueueAndJobSet {
		err := validation.ValidateQueueAndJobSet(req)
		if err != nil {
			return nil, err
		}
	} else {
		if req.JobSetId == "" || req.Queue == "" {
			ctx.
				WithField("apidatamissing", "true").
				Warnf("Reprioritize jobs called with missing data: jobId=%s, jobset=%s, queue=%s, user=%s", req.JobIds[0], req.JobSetId, req.Queue, s.GetUser(ctx))
		}

		// If either queue or jobSetId is missing, we get the job set and queue associated
		// with the first job id in the request.
		//
		// This must be done before checking auth, since the auth check expects a queue.
		if len(req.JobIds) > 0 && (req.Queue == "" || req.JobSetId == "") {
			firstJobId := req.JobIds[0]

			resolvedQueue, resolvedJobset, err := s.resolveQueueAndJobsetForJob(ctx, firstJobId)
			if err != nil {
				return nil, err
			}

			// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
			// since the job could not be found for the provided queue/jobSetId.
			// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
			// since the job could not be found for the provided queue/jobSetId.
			if req.Queue != "" && req.Queue != resolvedQueue {
				return nil, &armadaerrors.ErrNotFound{
					Type:    "job",
					Value:   firstJobId,
					Message: fmt.Sprintf("job not found in queue %s, try waiting", req.Queue),
				}
			}
			if req.JobSetId != "" && req.JobSetId != resolvedJobset {
				return nil, &armadaerrors.ErrNotFound{
					Type:    "job",
					Value:   firstJobId,
					Message: fmt.Sprintf("job not found in job set %s, try waiting", req.JobSetId),
				}
			}
			req.Queue = resolvedQueue
			req.JobSetId = resolvedJobset
		}
	}

	// TODO: this is incorrect we only validate the permissions on the first job but the other jobs may belong to different queues
	userId, groups, err := s.authorize(ctx, req.Queue, permissions.ReprioritizeAnyJobs, queue.PermissionVerbReprioritize)
	if err != nil {
		return nil, err
	}

	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue is empty",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "JobSetId is empty",
		}
	}
	priority := eventutil.LogSubmitPriorityFromApiPriority(req.NewPriority)

	// results maps job ids to strings containing error messages.
	results := make(map[string]string)

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId, // TODO: this is incorrect- the jobs may be for different jobsets
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, len(req.JobIds), len(req.JobIds)),
	}

	// No job ids implicitly indicates that all jobs in the job set should be re-prioritised.
	if len(req.JobIds) == 0 {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritiseJobSet{
				ReprioritiseJobSet: &armadaevents.ReprioritiseJobSet{
					Priority: priority,
				},
			},
		})

		results[fmt.Sprintf("all jobs in job set %s", req.JobSetId)] = ""
	}

	// Otherwise, only the specified jobs should be re-prioritised.
	for i, jobIdString := range req.JobIds {

		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdString)
		if err != nil {
			results[jobIdString] = err.Error()
			continue
		}

		sequence.Events[i] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &armadaevents.ReprioritiseJob{
					JobId:    jobId,
					Priority: priority,
				},
			},
		}

		results[jobIdString] = "" // empty string indicates no error
	}

	err = s.publisher.PublishMessages(ctx, sequence)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.JobReprioritizeResponse{
		ReprioritizationResults: results,
	}, nil
}

func (s *Server) CancelJobSet(grpcCtx context.Context, req *api.JobSetCancelRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue cannot be empty when cancelling a jobset",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "jobsetId cannot be empty when cancelling a jobset",
		}
	}

	err := validation.ValidateJobSetFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	userId, groups, err := s.authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	states := make([]armadaevents.JobState, len(req.GetFilter().GetStates()))
	for i := 0; i < len(states); i++ {
		switch req.GetFilter().GetStates()[i] {
		case api.JobState_PENDING:
			states[i] = armadaevents.JobState_PENDING
		case api.JobState_QUEUED:
			states[i] = armadaevents.JobState_QUEUED
		case api.JobState_RUNNING:
			states[i] = armadaevents.JobState_RUNNING
		}
	}
	pulsarSchedulerSequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: pointer.Now(),
				Event: &armadaevents.EventSequence_Event_CancelJobSet{
					CancelJobSet: &armadaevents.CancelJobSet{
						States: states,
						Reason: util.Truncate(req.Reason, 512),
					},
				},
			},
		},
	}
	err = s.publisher.PublishMessages(ctx, pulsarSchedulerSequence)
	if err != nil {
		log.WithError(err).Error("failed to send cancel jobset message to pulsar")
		return nil, status.Error(codes.Internal, "failed to send cancel jobset message to pulsar")
	}

	return &types.Empty{}, err
}

// Assumes all Job IDs are in the queue and job set provided
func (s *Server) cancelJobsByIdsQueueJobset(grpcCtx context.Context, jobIds []string, q, jobSet string, reason string) (*api.CancellationResult, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	if q == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: "Queue cannot be empty when cancelling multiple jobs",
		}
	}
	if jobSet == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Jobset",
			Value:   "",
			Message: "Jobset cannot be empty when cancelling multiple jobs",
		}
	}
	userId, groups, err := s.authorize(ctx, q, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}
	var cancelledIds []string
	es, cancelledIds := eventSequenceForJobIds(jobIds, q, jobSet, userId, groups, reason)
	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}
	return &api.CancellationResult{
		CancelledIds: cancelledIds,
	}, nil
}

// Returns event sequence along with all valid job ids in the sequence
func eventSequenceForJobIds(jobIds []string, q, jobSet, userId string, groups []string, reason string) (*armadaevents.EventSequence, []string) {
	sequence := &armadaevents.EventSequence{
		Queue:      q,
		JobSetName: jobSet,
		UserId:     userId,
		Groups:     groups,
		Events:     []*armadaevents.EventSequence_Event{},
	}
	var validIds []string
	for _, jobIdStr := range jobIds {
		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
		if err != nil {
			log.WithError(err).Errorf("could not convert job id to uuid: %s", jobIdStr)
			continue
		}
		validIds = append(validIds, jobIdStr)
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: pointer.Now(),
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{
					JobId:  jobId,
					Reason: util.Truncate(reason, 512),
				},
			},
		})
	}
	return sequence, validIds
}

// resolveQueueAndJobsetForJob returns the queue and jobset for a job.
// If no job can be retrieved then an error is returned.
func (s *Server) resolveQueueAndJobsetForJob(ctx *armadacontext.Context, jobId string) (string, string, error) {
	jobDetails, err := s.jobRepository.GetPulsarSchedulerJobDetails(ctx, jobId)
	if err != nil {
		return "", "", err
	}
	if jobDetails != nil {
		return jobDetails.Queue, jobDetails.JobSet, nil
	}
	return "", "", &armadaerrors.ErrNotFound{
		Type:  "job",
		Value: jobId,
	}
}

func (s *Server) CreateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
	}

	if len(req.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		req.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error validating queue: %s", err)
	}

	err = s.queueRepository.CreateQueue(ctx, queue)
	var eq *repository.ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "[CreateQueue] error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (s *Server) CreateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueCreateResponse
	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := s.CreateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueCreateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueCreateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (s *Server) UpdateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[UpdateQueue] error updating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error checking permissions: %s", err)
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	err = s.queueRepository.UpdateQueue(ctx, queue)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[UpdateQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func (s *Server) UpdateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueUpdateResponse

	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := s.UpdateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueUpdateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueUpdateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (s *Server) DeleteQueue(grpcCtx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := s.authorizer.AuthorizeAction(ctx, permissions.DeleteQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[DeleteQueue] error deleting queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[DeleteQueue] error checking permissions: %s", err)
	}
	err = s.queueRepository.DeleteQueue(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error deleting queue %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

func (s *Server) GetQueue(grpcCtx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	queue, err := s.queueRepository.GetQueue(ctx, req.Name)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[GetQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueue] error getting queue %q: %s", req.Name, err)
	}
	return queue.ToAPI(), nil
}

func (s *Server) GetQueues(req *api.StreamingQueueGetRequest, stream api.Submit_GetQueuesServer) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())

	// Receive once to get information about the number of queues to return
	numToReturn := req.GetNum()
	if numToReturn < 1 {
		numToReturn = math.MaxUint32
	}

	queues, err := s.queueRepository.GetAllQueues(ctx)
	if err != nil {
		return err
	}
	for i, queue := range queues {
		if uint32(i) < numToReturn {
			err := stream.Send(&api.StreamingQueueMessage{
				Event: &api.StreamingQueueMessage_Queue{Queue: queue.ToAPI()},
			})
			if err != nil {
				return err
			}
		}
	}
	err = stream.Send(&api.StreamingQueueMessage{
		Event: &api.StreamingQueueMessage_End{
			End: &api.EndMarker{},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// authorize authorizes a user request to submit a state transition message to the log.
// User information used for authorization is extracted from the provided context.
// Checks that the user has either anyPerm (e.g., permissions.SubmitAnyJobs) or perm (e.g., PermissionVerbSubmit) for this queue.
// Returns the userId and groups extracted from the context.
func (s *Server) authorize(
	ctx *armadacontext.Context,
	queueName string,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) (string, []string, error) {
	principal := authorization.GetPrincipal(ctx)
	userId := principal.GetName()
	groups := principal.GetGroupNames()
	q, err := s.queueCache.GetQueue(ctx, queueName)
	if err != nil {
		return userId, groups, err
	}
	err = s.authorizer.AuthorizeQueueAction(ctx, q, anyPerm, perm)
	return userId, groups, err
}

func (s *Server) GetUser(ctx *armadacontext.Context) string {
	principal := authorization.GetPrincipal(ctx)
	return principal.GetName()
}

func (s *Server) Health(_ context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}
