package server

import (
	"context"
	"errors"
	"time"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

type EventServer struct {
	permissions     authorization.PermissionChecker
	eventRepository repository.EventRepository
	queueRepository repository.QueueRepository
	eventStore      repository.EventStore
}

func NewEventServer(
	permissions authorization.PermissionChecker,
	eventRepository repository.EventRepository,
	eventStore repository.EventStore,
	queueRepository repository.QueueRepository) *EventServer {

	return &EventServer{
		permissions:     permissions,
		eventRepository: eventRepository,
		eventStore:      eventStore,
		queueRepository: queueRepository,
	}
}

func (s *EventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	if err := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[Report] error: %s", err)
	}
	return &types.Empty{}, s.eventStore.ReportEvents([]*api.EventMessage{message})
}

func (s *EventServer) ReportMultiple(ctx context.Context, message *api.EventList) (*types.Empty, error) {
	if err := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[ReportMultiple] error: %s", err)
	}
	return &types.Empty{}, s.eventStore.ReportEvents(message.Events)
}

// GetJobSetEvents streams back all events associated with a particular job set.
func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	q, err := s.queueRepository.GetQueue(request.Queue)
	var expected *repository.ErrQueueNotFound
	if errors.Is(err, expected) {
		return status.Errorf(codes.NotFound, "[GetJobSetEvents] Queue %s does not exist", request.Queue)
	} else if err != nil {
		return err
	}

	err = checkPermission(s.permissions, stream.Context(), permissions.WatchAllEvents)
	var globalPermErr *ErrNoPermission
	if errors.As(err, &globalPermErr) {
		err = checkQueuePermission(s.permissions, stream.Context(), q, queue.PermissionVerbWatch)
		var queuePermErr *ErrNoPermission
		if errors.As(err, &queuePermErr) {
			return status.Errorf(codes.PermissionDenied,
				"[GetJobSetEvents] error getting events for queue: %s, job set: %s: %s",
				request.Queue, request.Id, MergePermissionErrors(globalPermErr, queuePermErr))
		} else if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error checking permissions: %s", err)
		}
	} else if err != nil {
		return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error checking permissions: %s", err)
	}

	fromId := request.FromMessageId

	var timeout time.Duration = -1
	var stopAfter = ""
	if request.Watch {
		timeout = 5 * time.Second
	} else {
		lastId, err := s.eventRepository.GetLastMessageId(request.Queue, request.Id)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error getting ID of last message: %s", err)
		}
		stopAfter = lastId
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		messages, err := s.eventRepository.ReadEvents(request.Queue, request.Id, fromId, 500, timeout)
		if err != nil {
			return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error reading events: %s", err)
		}

		stop := len(messages) == 0
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

		if !request.Watch && stop {
			return nil
		}
	}
}

func (s *EventServer) Watch(req *api.WatchRequest, stream api.Event_WatchServer) error {
	watch := NewEventWatcher(s.eventRepository.ReadEvents, stream.Send).
		MustExist(s.eventRepository.ReadEvents).
		Authorize(s.queueRepository.GetQueue, principalHasQueuePermissions)

	return watch(stream.Context(), req)
}
