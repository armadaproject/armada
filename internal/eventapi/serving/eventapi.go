package serving

import (
	ctx "context"
	"math"

	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/eventapi/serving/apimessages"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// EventApi is responsible for serveing User requests for event messages
type EventApi struct {
	jobsetMapper        eventapi.JobsetMapper
	subscriptionManager SubscriptionManager
	sequenceManager     SequenceManager
}

func NewEventApi(jobsetMapper eventapi.JobsetMapper, subscriptionManager SubscriptionManager, sequenceManager SequenceManager) *EventApi {
	return &EventApi{
		jobsetMapper:        jobsetMapper,
		sequenceManager:     sequenceManager,
		subscriptionManager: subscriptionManager,
	}
}

// GetJobSetEvents Returns a stream of events from the events Db
// If request.Watch is set then the stream will only end when the user requests it, otherwise it will return all events
// present in the database when the request was made.
func (r *EventApi) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	// Extract Jobset
	jobsetId, err := r.jobsetMapper.Get(ctx.Background(), request.Queue, request.Id)
	if err != nil {
		return err
	}

	// Extract Sequence
	fromSequence, err := model.ParseExternalSeqNo(request.FromMessageId)
	if err != nil {
		return err
	}
	var upTo = int64(math.MaxInt64)
	if !request.Watch {
		upTo, err = r.getLastMessageId(request.Queue, request.Id)
		if err != nil {
			return err
		}
	}

	// We can short circuit if there are no valid messages to  retrieve
	if upTo == -1 || upTo <= fromSequence.Sequence {
		return nil
	}

	subscription := r.subscriptionManager.Subscribe(jobsetId, fromSequence.Sequence)
	defer r.subscriptionManager.Unsubscribe(subscription.SubscriptionId)
	decompressor, err := compress.NewZlibDecompressor()
	if err != nil {
		return err
	}
	for events := range subscription.Channel {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		for _, compressedEvent := range events {
			msgIndex := 0
			decompressedEvent, err := decompressor.Decompress(compressedEvent.Event)
			if err != nil {
				return err
			}
			dbEvent := &armadaevents.DatabaseSequence{}
			err = proto.Unmarshal(decompressedEvent, dbEvent)
			if err != nil {
				return err
			}
			// These fields are not present in the db messages, so we add them back here
			dbEvent.EventSequence.Queue = request.Queue
			dbEvent.EventSequence.JobSetName = request.Id
			apiEvents, err := apimessages.FromEventSequence(dbEvent.EventSequence)
			if err != nil {
				return err
			}
			for _, apiEvent := range apiEvents {
				externalSequenceNo := model.ExternalSeqNo{Sequence: compressedEvent.SeqNo, Index: msgIndex}
				if externalSequenceNo.IsAfter(fromSequence) {
					err = stream.Send(&api.EventStreamMessage{
						Id:      externalSequenceNo.ToString(),
						Message: apiEvent,
					})
					if err != nil {
						return status.Errorf(codes.Unavailable, "[GetJobSetEvents] error sending event: %s", err)
					}
				}
				msgIndex++
			}
			if compressedEvent.SeqNo >= upTo {
				return nil
			}
		}
	}
	return nil
}

// getLastMessageId returns the latests seq No for the given jibset or -1 if there are no seqNos
func (r *EventApi) getLastMessageId(queue, jobSetId string) (int64, error) {
	id, err := r.jobsetMapper.Get(ctx.Background(), queue, jobSetId)
	if err != nil {
		return -1, err
	}
	offset, present := r.sequenceManager.Get(id)
	if !present {
		offset = -1
	}
	return offset, nil
}
