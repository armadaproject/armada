package server

import (
	"context"
	"time"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"

	"github.com/gogo/protobuf/types"
)

type EventServer struct {
	permissions     authorization.PermissionChecker
	eventRepository repository.EventRepository
	eventStore      repository.EventStore
}

func NewEventServer(
	permissions authorization.PermissionChecker,
	eventRepository repository.EventRepository,
	eventStore repository.EventStore) *EventServer {

	return &EventServer{
		permissions:     permissions,
		eventRepository: eventRepository,
		eventStore:      eventStore}
}

func (s *EventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	if e := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	return &types.Empty{}, s.eventStore.ReportEvents([]*api.EventMessage{message})
}

func (s *EventServer) ReportMultiple(ctx context.Context, message *api.EventList) (*types.Empty, error) {
	if e := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	return &types.Empty{}, s.eventStore.ReportEvents(message.Events)
}

func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	if e := checkPermission(s.permissions, stream.Context(), permissions.WatchAllEvents); e != nil {
		return e
	}

	fromId := request.FromMessageId

	var timeout time.Duration = -1
	var stopAfter = ""
	if request.Watch {
		timeout = 5 * time.Second
	} else {
		lastId, e := s.eventRepository.GetLastMessageId(request.Queue, request.Id)
		if e != nil {
			return e
		}
		stopAfter = lastId
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		messages, e := s.eventRepository.ReadEvents(request.Queue, request.Id, fromId, 500, timeout)

		if e != nil {
			return e
		}

		stop := len(messages) == 0
		for _, msg := range messages {
			fromId = msg.Id
			if fromId == stopAfter {
				stop = true
			}
			e = stream.Send(msg)
			if e != nil {
				return e
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
		Authorize(s.permissions.UserHasPermission, permissions.WatchAllEvents)

	return watch(stream.Context(), req)
}
