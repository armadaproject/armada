package server

import (
	"context"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/repository/sequence"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type EventServer struct {
	authorizer      ActionAuthorizer
	eventRepository repository.EventRepository
	queueRepository repository.QueueRepository
	jobRepository   repository.JobRepository
}

func NewEventServer(
	authorizer ActionAuthorizer,
	eventRepository repository.EventRepository,
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
) *EventServer {
	return &EventServer{
		authorizer:      authorizer,
		eventRepository: eventRepository,
		queueRepository: queueRepository,
		jobRepository:   jobRepository,
	}
}

// GetJobSetEvents streams back all events associated with a particular job set.
func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())
	q, err := s.queueRepository.GetQueue(ctx, request.Queue)
	var expected *repository.ErrQueueNotFound
	if errors.As(err, &expected) {
		return status.Errorf(codes.NotFound, "[GetJobSetEvents] Queue %s does not exist", request.Queue)
	} else if err != nil {
		return err
	}

	err = validateUserHasWatchPermissions(ctx, s.authorizer, q, request.Id)
	if err != nil {
		return status.Errorf(codes.PermissionDenied, "[GetJobSetEvents] %s", err)
	}

	// convert the seqNo over if necessary
	if !sequence.IsValid(request.FromMessageId) {
		convertedSeqId, err := sequence.FromRedisId(request.FromMessageId, 0, true)
		if err != nil {
			return errors.Wrapf(err, "Could not convert legacy message id over to new message id for request for queue %s, jobset %s", request.Queue, request.Id)
		}
		log.Warnf("Converted legacy sequene id [%s] for queues %s, jobset %s to new sequenceId [%s]", request.Id, request.Queue, request.Id, convertedSeqId)
		request.FromMessageId = convertedSeqId.String()
	}

	return s.serveEventsFromRepository(request, s.eventRepository, stream)
}

func (s *EventServer) Health(_ context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func (s *EventServer) Watch(req *api.WatchRequest, stream api.Event_WatchServer) error {
	request := &api.JobSetRequest{
		Id:             req.JobSetId,
		Watch:          true,
		FromMessageId:  req.FromId,
		Queue:          req.Queue,
		ErrorIfMissing: true,
		ForceLegacy:    req.ForceLegacy,
		ForceNew:       req.ForceNew,
	}
	return s.GetJobSetEvents(request, stream)
}

func (s *EventServer) serveEventsFromRepository(request *api.JobSetRequest, eventRepository repository.EventRepository,
	stream api.Event_GetJobSetEventsServer,
) error {
	ctx := armadacontext.FromGrpcCtx(stream.Context())
	if request.ErrorIfMissing {
		exists, err := eventRepository.CheckStreamExists(ctx, request.Queue, request.Id)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error when checking jobset exists: %s", err)
		}
		if !exists {
			return status.Errorf(codes.NotFound, "[GetJobSetEvents] Jobset %s for queue %s does not exist", request.Id, request.Queue)
		}
	}

	fromId := request.FromMessageId

	var timeout time.Duration = -1
	stopAfter := ""
	if request.Watch {
		timeout = 5 * time.Second
	} else {
		lastId, err := eventRepository.GetLastMessageId(ctx, request.Queue, request.Id)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error getting ID of last message: %s", err)
		}
		stopAfter = lastId
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		messages, lastMessageId, err := eventRepository.ReadEvents(ctx, request.Queue, request.Id, fromId, 500, timeout)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error reading events: %s", err)
		}

		stop := len(messages) == 0
		if len(messages) == 0 {
			if lastMessageId != nil {
				fromId = lastMessageId.String()
			}
		} else {
			for _, msg := range messages {
				fromId = msg.Id
				if fromId == stopAfter {
					stop = true
				}
				err = stream.Send(msg)
				if err != nil {
					return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error sending event: %s", err)
				}
			}
		}

		if !request.Watch && stop {
			return nil
		}
	}
}

func validateUserHasWatchPermissions(ctx *armadacontext.Context, authorizer ActionAuthorizer, q queue.Queue, jobSetId string) error {
	err := authorizer.AuthorizeQueueAction(ctx, q, permissions.WatchAllEvents, queue.PermissionVerbWatch)
	var permErr *armadaerrors.ErrUnauthorized
	if errors.As(err, &permErr) {
		return status.Errorf(codes.PermissionDenied, "error getting events for queue: %s, job set: %s: %s",
			q.Name, jobSetId, permErr)
	} else if err != nil {
		return status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}
	return nil
}
