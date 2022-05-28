package ingestion

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// MessageRowConverter raw converts pulsar messages into events that we can insert into the database
type MessageRowConverter struct {
	eventDb    *eventdb.EventDb
	compressor compress.Compressor
}

// Convert takes a  channel of pulsar message batches and outputs a channel of batched events that we can insert into the database
func Convert(ctx context.Context, msgs chan []*pulsarutils.ConsumerMessage, bufferSize int, converter *MessageRowConverter) chan *model.BatchUpdate {
	out := make(chan *model.BatchUpdate, bufferSize)
	go func() {
		for pulsarBatch := range msgs {
			out <- converter.ConvertBatch(ctx, pulsarBatch)
		}
		close(out)
	}()
	return out
}

func (rc *MessageRowConverter) ConvertBatch(ctx context.Context, batch []*pulsarutils.ConsumerMessage) *model.BatchUpdate {

	// First unmarshall everything
	messageIds := make([]*pulsarutils.ConsumerMessageId, len(batch))
	eventSequences := make([]*armadaevents.EventSequence, len(batch))
	distinctJobsets := make(map[model.QueueJobsetPair]bool)
	for i, msg := range batch {

		pulsarMsg := msg.Message

		// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
		// As they must be acked at the end
		messageIds[i] = &pulsarutils.ConsumerMessageId{MessageId: pulsarMsg.ID(), ConsumerId: msg.ConsumerId}

		// If it's not a control message then ignore
		if !armadaevents.IsControlMessage(pulsarMsg) {
			continue
		}

		//  If there's no index on the message ignore
		if pulsarMsg.Index() == nil {
			log.Warnf("Index not found on pulsar message %s. Ignoring", pulsarMsg.ID())
			continue
		}

		// Try and unmarshall the proto
		es, err := eventutil.UnmarshalEventSequence(ctx, msg.Message.Payload())
		if err != nil {
			log.WithError(err).Warnf("Could not unmarshal proto for msg %s", pulsarMsg.ID())
			continue
		}

		// Fill in the created time if it's missing
		// TODO: we can remove this once created is being populated everywhere
		for _, event := range es.Events {
			if event.Created == nil {
				t := msg.Message.PublishTime().In(time.UTC)
				event.Created = &t
			}
		}
		eventSequences[i] = es
		distinctJobsets[model.QueueJobsetPair{Queue: es.Queue, Jobset: es.JobSetName}] = true
	}

	// Map QueueJobsetPairs to ints
	mappings, err := rc.eventDb.GetOrCreateJobsetIds(ctx, distinctJobsets)
	if err != nil {
		log.WithError(err).Warnf("Error mapping jobsets")
		return &model.BatchUpdate{MessageIds: messageIds, Events: nil}
	}

	// Finally construct event ros
	eventRows := make([]*model.EventRow, 0)
	for i, es := range eventSequences {

		if es != nil {

			jobsetId, present := mappings[model.QueueJobsetPair{Queue: es.Queue, Jobset: es.JobSetName}]

			if !present {
				log.Warnf("Could not map queue=%s, jobset=%s to internal id. Skipping msg %s", es.Queue, es.JobSetName, batch[i].Message.ID())
				continue
			}

			// Remove the jobset Name and the queue from the proto as we're storing this in the db
			es.JobSetName = ""
			es.Queue = ""

			dbEvent := &armadaevents.DatabaseSequence{EventSequence: es}

			bytes, err := proto.Marshal(dbEvent)
			if err != nil {

			}
			protoBytes, err := rc.compressor.Compress(bytes)
			if err != nil {
				log.WithError(err).Warnf("Could not compress proto for msg %s", batch[i].Message.ID())
			}
			eventRows = append(eventRows, &model.EventRow{
				JobSetId: jobsetId,
				SeqNo:    int64(*batch[i].Message.Index()),
				Event:    protoBytes,
			})
		}

	}

	return &model.BatchUpdate{MessageIds: messageIds, Events: eventRows}
}
