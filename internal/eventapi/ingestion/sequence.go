package ingestion

import (
	"context"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// SendSequenceUpdates takes a channel of events that have been processed and publishes the corresponding sequence numbers onto pulsar
// It outputs the pulsar message ids of the originating events so that the messages can be accked
func SendSequenceUpdates(ctx context.Context, producer pulsar.Producer, msgs chan *model.BatchUpdate, bufferSize int) chan []*pulsarutils.ConsumerMessageId {
	out := make(chan []*pulsarutils.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			SendSequenceUpdate(ctx, msg.Events, producer)
			out <- msg.MessageIds
		}
		close(out)
	}()
	return out
}

// SendSequenceUpdate synchronously sends sequence numbers to Pulsar
// TODO: Retries if the pulsar send fails
func SendSequenceUpdate(ctx context.Context, inputMsgs []*model.EventRow, producer pulsar.Producer) {

	if len(inputMsgs) == 0 {
		return
	}

	seqUpdates := make([]*armadaevents.SeqUpdate, len(inputMsgs))
	for i, event := range inputMsgs {
		seqUpdates[i] = &armadaevents.SeqUpdate{
			JobsetId: event.JobSetId,
			SeqNo:    event.SeqNo,
		}
	}

	offsetsBatch := &armadaevents.SeqUpdates{
		Updates: seqUpdates,
	}
	payload, err := proto.Marshal(offsetsBatch)
	if err == nil {
		msg := &pulsar.ProducerMessage{
			Payload: payload,
		}
		sent := false
		for sent == false {
			_, err := producer.Send(ctx, msg)
			if err == nil {
				sent = true
			} else {
				log.WithError(errors.WithStack(err)).Warnf("Error sending update message")
			}
		}
	} else {
		log.WithError(errors.WithStack(err)).Warnf("Error marshalling sequence update")
	}
}
