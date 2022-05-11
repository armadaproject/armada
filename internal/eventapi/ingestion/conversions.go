package ingestion

import (
	"context"
	"fmt"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
)

type MessageRowConverter struct {
	jobsetMapper eventapi.JobsetMapper
	compressor   compress.Compressor
}

func Convert(ctx context.Context, msgs chan *pulsarutils.ConsumerMessage, bufferSize int, converter *MessageRowConverter) chan *model.PulsarEventRow {
	out := make(chan *model.PulsarEventRow, bufferSize)
	go func() {
		for msg := range msgs {
			event, err := converter.ConvertMsg(ctx, msg)
			if err != nil {
				out <- event
			}
		}
		close(out)
	}()
	return out
}

func (rc *MessageRowConverter) ConvertMsg(ctx context.Context, msg *pulsarutils.ConsumerMessage) (*model.PulsarEventRow, error) {

	pulsarMsg := msg.Message

	// Try and resolve an index. We require this
	if msg.Message.Index() == nil {
		return nil, fmt.Errorf("index not found on pulsar message")
	}

	var event proto.Message
	var jobsetId int64
	var err error
	if armadaevents.IsControlMessage(pulsarMsg) {
		event, jobsetId, err = rc.handleControlMessage(context.Background(), pulsarMsg)
	} else {
		event, jobsetId, err = rc.handleUtilisationMessage(context.Background(), pulsarMsg)
	}

	if err != nil {
		return nil, err
	}

	bytes, err := proto.Marshal(event)
	if err != nil {
		return nil, err
	}
	protoBytes, err := rc.compressor.Compress(bytes)
	if err != nil {
		return nil, err
	}

	return &model.PulsarEventRow{
		MessageId: &pulsarutils.ConsumerMessageId{
			MessageId:  pulsarMsg.ID(),
			Index:      int64(*pulsarMsg.Index()),
			ConsumerId: msg.ConsumerId,
		},
		Event: &model.EventRow{
			JobSetId: jobsetId,
			SeqNo:    int64(*pulsarMsg.Index()),
			Event:    protoBytes,
		},
	}, nil
}

func (rc *MessageRowConverter) handleControlMessage(ctx context.Context, msg pulsar.Message) (proto.Message, int64, error) {

	// Try and unmarshall the proto-  if it fails there's not much we can do here.
	event, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
	if err != nil {
		return nil, -1, err
	}

	// Try and resolve a jobsetId
	jobsetId, err := rc.jobsetMapper.Get(ctx, event.Queue, event.JobSetName)
	if err != nil {
		return nil, -1, err
	}

	// remove the jobset Name and the queue from the proto as we're storing this in the db
	event.JobSetName = ""
	event.Queue = ""
	return &armadaevents.DatabaseEvent{Event: &armadaevents.DatabaseEvent_EventSequence{EventSequence: event}}, jobsetId, nil
}

func (rc *MessageRowConverter) handleUtilisationMessage(ctx context.Context, msg pulsar.Message) (*armadaevents.DatabaseEvent, int64, error) {

	// Try and unmarshall the proto-  if it fails there's not much we can do here.
	event := &armadaevents.JobUtilisationEvent{}
	err := proto.Unmarshal(msg.Payload(), event)
	if err != nil {
		return nil, -1, err
	}

	// Try and resolve a jobsetId
	jobsetId, err := rc.jobsetMapper.Get(ctx, event.Queue, event.JobSetId)
	if err != nil {
		return nil, -1, err
	}

	// remove the jobset Name and the queue from the proto as we're storing this in the db
	event.JobSetId = ""
	event.Queue = ""
	return &armadaevents.DatabaseEvent{Event: &armadaevents.DatabaseEvent_JobUtilisation{JobUtilisation: event}}, jobsetId, nil
}
