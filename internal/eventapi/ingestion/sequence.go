package ingestion

import (
	"context"
	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/log"

	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// SendSequenceUpdates takes a channel of events that have been processed and publishes the corresponding sequence numbers onto pulsar
// It outputs the pulsar message ids of the originating events so that the messages can be accked
func SendSequenceUpdates(ctx context.Context, producer pulsar.Producer, msgs chan []*model.PulsarEventRow, bufferSize int) chan []*pulsarutils.ConsumerMessageId {
	out := make(chan []*pulsarutils.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			msgIds := SendSequenceUpdate(ctx, msg, producer)
			out <- msgIds
		}
		close(out)
	}()
	return out
}

// SendSequenceUpdate synchronously sends sequence numbers to Pulsar
// TODO: Retries if the pulsar send fails
func SendSequenceUpdate(ctx context.Context, inputMsgs []*model.PulsarEventRow, producer pulsar.Producer) []*pulsarutils.ConsumerMessageId {
	seqUpdates := make([]*armadaevents.SeqUpdate, len(inputMsgs))
	messageIds := make([]*pulsarutils.ConsumerMessageId, len(inputMsgs))
	for i := 0; i < len(messageIds); i++ {
		messageIds[i] = inputMsgs[i].MessageId
		seqUpdates[i] = &armadaevents.SeqUpdate{
			JobsetId: inputMsgs[i].Event.JobSetId,
			SeqNo:    inputMsgs[i].MessageId.Index,
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
				log.Warnf("Error sending update message %+v", err)
			}
		}
	} else {
		log.Warnf("Error marshalling event")
	}
	return messageIds
}
