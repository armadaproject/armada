package convert

import (
	"context"

	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/eventingester/model"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// MessageRowConverter converts raw pulsar messages into events that we can store in Redis
type MessageRowConverter struct {
	Compressor          compress.Compressor
	MaxMessageBatchSize int
}

// Convert takes a channel of pulsar message batches and outputs a channel of batched events that we store in Redis
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
	events := make([]*model.Event, 0, len(batch))

	sequences := make([]*armadaevents.EventSequence, 0, len(batch))
	for i, msg := range batch {
		pulsarMsg := msg.Message

		// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
		// As they must be acked at the end
		messageIds[i] = &pulsarutils.ConsumerMessageId{MessageId: pulsarMsg.ID(), ConsumerId: msg.ConsumerId}

		// If it's not a control message then ignore
		if !armadaevents.IsControlMessage(pulsarMsg) {
			continue
		}

		// Try and unmarshall the proto
		es, err := eventutil.UnmarshalEventSequence(ctx, msg.Message.Payload())
		if err != nil {
			log.WithError(err).Warnf("Could not unmarshal proto for msg %s", pulsarMsg.ID())
			continue
		}

		// Fill in time if it is not set
		// TODO - once created is set everywhere we can remove this
		for _, event := range es.Events {
			if event.GetCreated() == nil {
				publishTime := pulsarMsg.PublishTime()
				event.Created = &publishTime
			}
		}

		// cut out groups:
		es.Groups = nil

		sequences = append(sequences, es)
	}
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, rc.MaxMessageBatchSize, false)
	if err != nil {
		log.WithError(err).Errorf("Failed to compact sequences")
		return &model.BatchUpdate{
			MessageIds: messageIds,
			Events:     events,
		}
	}

	for i, es := range sequences {
		// Remove the jobset Name and the queue from the proto as this will be stored as the key
		queue := es.Queue
		jobset := es.JobSetName
		es.JobSetName = ""
		es.Queue = ""

		bytes, err := proto.Marshal(es)
		if err != nil {
			log.WithError(err).Warnf("Could not marshall proto for msg %s", batch[i].Message.ID())
			continue
		}
		compressedBytes, err := rc.Compressor.Compress(bytes)
		if err != nil {
			log.WithError(err).Warnf("Could not compress event for msg %s", batch[i].Message.ID())
			continue
		}

		events = append(events, &model.Event{
			Queue:  queue,
			Jobset: jobset,
			Event:  compressedBytes,
		})
	}

	return &model.BatchUpdate{
		MessageIds: messageIds,
		Events:     events,
	}
}
