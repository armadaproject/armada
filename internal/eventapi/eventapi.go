package eventapi

import (
	ctx "context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/pkg/api"
)

type PostgresEventRepository struct {
	mapper              *JobsetMapper
	subscriptionManager *SubscriptionManager
	offsets             *OffsetManager
}

func (r *PostgresEventRepository) CheckStreamExists(queue string, jobSetId string) (bool, error) {
	return true, nil
}

func (r *PostgresEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	id, err := r.mapper.get(ctx.Background(), queue, jobSetId)
	if err != nil {
		return "", err
	}
	offset, _ := r.offsets.Get(id)
	return string(offset), nil
}

func (r *PostgresEventRepository) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {

	jobsetId, err := r.mapper.get(ctx.Background(), request.Queue, request.Id)
	if err != nil {
		return err
	}

	fromId := request.FromMessageId

	var timeout time.Duration = -1
	var stopAfter = -1

	if !request.Watch {
		stopAfter, _ := r.offsets.Get(jobsetId)
	}

	subscription := r.subscriptionManager.Subscribe(jobsetId)
	defer r.subscriptionManager.Unsubscribe(subscription.SubscriptionId)

	for msg := range subscription.Channel {

		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		// todo convert events
		stream.Send()
	}

	for {

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
