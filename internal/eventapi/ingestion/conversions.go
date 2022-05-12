package ingestion

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
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

	// Try and unmarshall the proto-  if it fails there's not much we can do here.
	event, err := eventutil.UnmarshalEventSequence(ctx, msg.Message.Payload())
	if err != nil {
		return nil, err
	}

	// Try and resolve a jobsetId
	jobsetId, err := rc.jobsetMapper.Get(ctx, event.Queue, event.JobSetName)
	if err != nil {
		return nil, err
	}

	// Remove the jobset Name and the queue from the proto as we're storing this in the db
	event.JobSetName = ""
	event.Queue = ""
	dbEvent := &armadaevents.DatabaseEvent{EventSequence: event, Time: msg.Message.PublishTime()}

	bytes, err := proto.Marshal(dbEvent)
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
