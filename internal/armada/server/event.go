package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"time"

	"github.com/gogo/protobuf/types"
)

type EventServer struct {
	eventRepository repository.EventRepository
}

func NewEventServer(eventRepository repository.EventRepository) *EventServer {
	return &EventServer{eventRepository: eventRepository}
}

func (s *EventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	return &types.Empty{}, s.eventRepository.ReportEvent(message)
}

func (s *EventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {

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

		messages, e := s.eventRepository.ReadEvents(request.Id, lastId, 100, timeout)

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
