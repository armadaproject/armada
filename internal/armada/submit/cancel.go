package submit

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

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

	// resolve the queue and jobset of the job: we can't trust what the user has given us
	resolvedQueue, resolvedJobset, err := s.resolveQueueAndJobsetForJob(req.JobId)
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
	if req.JobSetId != "" && req.JobSetId != resolvedJobset {
		return nil, &armadaerrors.ErrNotFound{
			Type:    "job",
			Value:   req.JobId,
			Message: fmt.Sprintf("job not found in job set %s, try waiting", req.JobSetId),
		}
	}

	userId, groups, err := s.Authorize(ctx, resolvedQueue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
	if err != nil {
		return nil, err
	}

	sequence := &armadaevents.EventSequence{
		Queue:      resolvedQueue,
		JobSetName: resolvedJobset,
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
	err = pulsarutils.CompactAndPublishSequences(
		ctx,
		[]*armadaevents.EventSequence{sequence},
		s.Producer, s.MaxAllowedMessageSize,
		schedulers.Pulsar)

	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: []string{req.JobId}, // indicates no error
	}, nil
}

func (s *Server) ReprioritizeJobs(grpcCtx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

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

		resolvedQueue, resolvedJobset, err := s.resolveQueueAndJobsetForJob(firstJobId)
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

	// TODO: this is incorrect we only validate the permissions on the first job but the other jobs may belong to different queues
	userId, groups, err := s.Authorize(ctx, req.Queue, permissions.ReprioritizeAnyJobs, queue.PermissionVerbReprioritize)
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

	err = pulsarutils.CompactAndPublishSequences(
		ctx,
		[]*armadaevents.EventSequence{sequence},
		s.Producer, s.MaxAllowedMessageSize,
		schedulers.Pulsar)

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

	userId, groups, err := s.Authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
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
	err = pulsarutils.CompactAndPublishSequences(
		ctx,
		[]*armadaevents.EventSequence{pulsarSchedulerSequence},
		s.Producer, s.MaxAllowedMessageSize,
		schedulers.Pulsar)
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
	userId, groups, err := s.Authorize(ctx, q, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}
	var cancelledIds []string
	es, cancelledIds := eventSequenceForJobIds(jobIds, q, jobSet, userId, groups, reason)
	err = pulsarutils.CompactAndPublishSequences(
		ctx,
		[]*armadaevents.EventSequence{es},
		s.Producer, s.MaxAllowedMessageSize,
		schedulers.Pulsar)
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
func (s *Server) resolveQueueAndJobsetForJob(jobId string) (string, string, error) {
	jobDetails, err := s.JobRepository.GetPulsarSchedulerJobDetails(jobId)
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
