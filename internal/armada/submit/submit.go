package submit

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	armadaqueue "github.com/armadaproject/armada/internal/armada/queue"
	"github.com/armadaproject/armada/internal/armada/submit/conversion"
	"github.com/armadaproject/armada/internal/armada/submit/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// Server is a service that accepts API calls according to the original Armada submit API and publishes messages
// to Pulsar based on those calls.
type Server struct {
	queueService     api.QueueServiceServer
	publisher        pulsarutils.Publisher
	queueCache       armadaqueue.ReadOnlyQueueRepository
	submissionConfig configuration.SubmissionConfig
	deduplicator     Deduplicator
	authorizer       auth.ActionAuthorizer
	// Below are used only for testing
	clock       clock.Clock
	idGenerator func() *armadaevents.Uuid
}

func NewServer(
	queueService api.QueueServiceServer,
	publisher pulsarutils.Publisher,
	queueCache armadaqueue.ReadOnlyQueueRepository,
	submissionConfig configuration.SubmissionConfig,
	deduplicator Deduplicator,
	authorizer auth.ActionAuthorizer,
) *Server {
	return &Server{
		queueService:     queueService,
		publisher:        publisher,
		queueCache:       queueCache,
		submissionConfig: submissionConfig,
		deduplicator:     deduplicator,
		authorizer:       authorizer,
		clock:            clock.RealClock{},
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
	jobIds := []string{}
	jobIds = append(jobIds, req.JobIds...)
	if req.JobId != "" {
		jobIds = append(jobIds, req.JobId)
	}
	jobIds = slices.Unique(jobIds)

	if len(jobIds) == 0 {
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

	err := validation.ValidateQueueAndJobSet(req)
	if err != nil {
		return nil, err
	}

	userId, groups, err := s.authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	var cancelledIds []string
	es, cancelledIds := eventSequenceForJobIds(s.clock, jobIds, req.Queue, req.JobSetId, userId, groups, req.Reason)

	err = s.publisher.PublishMessages(ctx, es)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}
	return &api.CancellationResult{
		CancelledIds: cancelledIds,
	}, nil
}

func (s *Server) PreemptJobs(grpcCtx context.Context, req *api.JobPreemptRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := validation.ValidateQueueAndJobSet(req)
	if err != nil {
		return nil, err
	}

	userId, groups, err := s.authorize(ctx, req.Queue, permissions.PreemptAnyJobs, queue.PermissionVerbPreempt)
	if err != nil {
		return nil, err
	}

	sequence, err := preemptJobEventSequenceForJobIds(s.clock, req.JobIds, req.Queue, req.JobSetId, userId, groups)
	if err != nil {
		return nil, err
	}

	err = s.publisher.PublishMessages(ctx, sequence)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &types.Empty{}, nil
}

func preemptJobEventSequenceForJobIds(clock clock.Clock, jobIds []string, q, jobSet, userId string, groups []string) (*armadaevents.EventSequence, error) {
	sequence := &armadaevents.EventSequence{
		Queue:      q,
		JobSetName: jobSet,
		UserId:     userId,
		Groups:     groups,
		Events:     []*armadaevents.EventSequence_Event{},
	}
	eventTime := clock.Now().UTC()
	for _, jobIdStr := range jobIds {
		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
		if err != nil {
			log.WithError(err).Errorf("could not convert job id to uuid: %s", jobIdStr)
			return nil, fmt.Errorf("could not convert job id to uuid: %s", jobIdStr)
		}
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_JobPreemptionRequested{
				JobPreemptionRequested: &armadaevents.JobPreemptionRequested{
					JobId:    jobId,
					JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				},
			},
		})
	}
	return sequence, nil
}

func (s *Server) ReprioritizeJobs(grpcCtx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := validation.ValidateQueueAndJobSet(req)
	if err != nil {
		return nil, err
	}

	userId, groups, err := s.authorize(ctx, req.Queue, permissions.ReprioritizeAnyJobs, queue.PermissionVerbReprioritize)
	if err != nil {
		return nil, err
	}

	// results maps job ids to strings containing error messages.
	results := make(map[string]string)
	priority := conversion.PriorityAsInt32(req.NewPriority)

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, len(req.JobIds), len(req.JobIds)),
	}

	eventTime := s.clock.Now().UTC()
	// No job ids implicitly indicates that all jobs in the job set should be re-prioritised.
	if len(req.JobIds) == 0 {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &eventTime,
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
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &armadaevents.ReprioritiseJob{
					JobId:    jobId,
					JobIdStr: jobIdString,
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
	err := validation.ValidateQueueAndJobSet(req)
	if err != nil {
		return nil, err
	}

	err = validation.ValidateJobSetFilter(req.Filter)
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
	eventTime := s.clock.Now().UTC()
	pulsarSchedulerSequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: &eventTime,
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

// Returns event sequence along with all valid job ids in the sequence
func eventSequenceForJobIds(clock clock.Clock, jobIds []string, queue, jobSet, userId string, groups []string, reason string) (*armadaevents.EventSequence, []string) {
	sequence := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobSet,
		UserId:     userId,
		Groups:     groups,
		Events:     []*armadaevents.EventSequence_Event{},
	}
	var validIds []string
	truncatedReason := util.Truncate(reason, 512)
	eventTime := clock.Now().UTC()
	for _, jobIdStr := range jobIds {
		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
		if err != nil {
			log.WithError(err).Errorf("could not convert job id to uuid: %s", jobIdStr)
			continue
		}
		validIds = append(validIds, jobIdStr)
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{
					JobId:    jobId,
					JobIdStr: jobIdStr,
					Reason:   truncatedReason,
				},
			},
		})
	}
	return sequence, validIds
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
	principal := auth.GetPrincipal(ctx)
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
	principal := auth.GetPrincipal(ctx)
	return principal.GetName()
}

func (s *Server) Health(_ context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

// Functions below are deprecated

func (s *Server) CreateQueue(ctx context.Context, q *api.Queue) (*types.Empty, error) {
	return s.queueService.CreateQueue(ctx, q)
}

func (s *Server) CreateQueues(ctx context.Context, list *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	return s.queueService.CreateQueues(ctx, list)
}

func (s *Server) UpdateQueue(ctx context.Context, q *api.Queue) (*types.Empty, error) {
	return s.queueService.UpdateQueue(ctx, q)
}

func (s *Server) UpdateQueues(ctx context.Context, list *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	return s.queueService.UpdateQueues(ctx, list)
}

func (s *Server) DeleteQueue(ctx context.Context, request *api.QueueDeleteRequest) (*types.Empty, error) {
	return s.queueService.DeleteQueue(ctx, request)
}

func (s *Server) GetQueue(ctx context.Context, request *api.QueueGetRequest) (*api.Queue, error) {
	return s.queueService.GetQueue(ctx, request)
}

func (s *Server) GetQueues(request *api.StreamingQueueGetRequest, server api.Submit_GetQueuesServer) error {
	return s.queueService.GetQueues(request, server)
}
