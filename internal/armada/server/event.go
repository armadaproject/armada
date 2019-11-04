package server

import (
	"context"
	"time"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/repository"

	"github.com/gogo/protobuf/types"
)

type EventServer struct {
	permissions     authorization.PermissionChecker
	eventRepository repository.EventRepository
}

func NewEventServer(permissions authorization.PermissionChecker, eventRepository repository.EventRepository) *EventServer {
	return &EventServer{permissions: permissions, eventRepository: eventRepository}
}

func (s *EventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	if e := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	return &types.Empty{}, s.eventRepository.ReportEvent(message)
}

func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	if e := checkPermission(s.permissions, stream.Context(), permissions.WatchAllEvents); e != nil {
		return e
	}

	lastId := request.FromMessageId

	var timeout time.Duration = -1
	if request.Watch {
		timeout = 5 * time.Second
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		messages, e := s.eventRepository.ReadEvents(request.Id, lastId, 500, timeout)

		if e != nil {
			return e
		}
		for _, msg := range messages {
			lastId = msg.Id
			e = stream.Send(msg)
			if e != nil {
				return e
			}
		}

		if !request.Watch && len(messages) == 0 {
			return nil
		}
	}
}
