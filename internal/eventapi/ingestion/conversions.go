package ingestion

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// MessageRowConverter raw converts pulsar messages into events that we can insert into the database
type MessageRowConverter struct {
	jobsetMapper eventapi.JobsetMapper
	compressor   compress.Compressor
}

// Convert takes a  channel of pulsar messages and outputs a channel of events that we can insert into the database
func Convert(ctx context.Context, msgs chan *pulsarutils.ConsumerMessage, bufferSize int, converter *MessageRowConverter) chan *model.PulsarEventRow {
	out := make(chan *model.PulsarEventRow, bufferSize)
	go func() {
		for msg := range msgs {
			event, err := converter.ConvertMsg(ctx, msg)
			if err != nil {
				log.Warnf("Error processing message %s: %+v", msg.Message.ID(), err)
			}
			out <- event
		}
		close(out)
	}()
	return out
}

//ConvertMsg converts pulsar messages into events that we can insert into the database
func (rc *MessageRowConverter) ConvertMsg(ctx context.Context, msg *pulsarutils.ConsumerMessage) (*model.PulsarEventRow, error) {

	pulsarMsg := msg.Message

	// If it's not a control message then just propagate the message
	if !armadaevents.IsControlMessage(pulsarMsg) {
		return emptyEvent(msg), nil
	}

	// Try and resolve an index. We require this
	if msg.Message.Index() == nil {
		return emptyEvent(msg), fmt.Errorf("index not found on pulsar message")
	}

	// Try and unmarshall the proto-  if it fails there's not much we can do here.
	es, err := eventutil.UnmarshalEventSequence(ctx, msg.Message.Payload())
	if err != nil {
		return emptyEvent(msg), err
	}

	// Try and resolve a jobsetId
	jobsetId, err := rc.jobsetMapper.Get(ctx, es.Queue, es.JobSetName)
	if err != nil {
		return emptyEvent(msg), err
	}

	// Fill in the created time if it's missing
	// TODO: we can remove this once created is being populated everywhere
	for _, event := range es.Events {
		if event.Created == nil {
			t := msg.Message.PublishTime()
			event.Created = &t
		}
	}

	// Remove the jobset Name and the queue from the proto as we're storing this in the db
	es.JobSetName = ""
	es.Queue = ""
	dbEvent := &armadaevents.DatabaseEvent{EventSequence: es}

	bytes, err := proto.Marshal(dbEvent)
	if err != nil {
		return emptyEvent(msg), err
	}
	protoBytes, err := rc.compressor.Compress(bytes)
	if err != nil {
		return emptyEvent(msg), err
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

func emptyEvent(msg *pulsarutils.ConsumerMessage) *model.PulsarEventRow {
	return &model.PulsarEventRow{
		MessageId: &pulsarutils.ConsumerMessageId{
			MessageId:  msg.Message.ID(),
			Index:      int64(*msg.Message.Index()),
			ConsumerId: msg.ConsumerId,
		},
	}
}
