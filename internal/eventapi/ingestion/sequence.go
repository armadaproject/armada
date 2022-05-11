package ingestion

import (
	"context"
	model2 "github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
)

func SendSequenceUpdates(ctx context.Context, producer pulsar.Producer, msgs chan []*model2.PulsarEventRow, bufferSize int) chan []*model.ConsumerMessageId {
	out := make(chan []*model.ConsumerMessageId, bufferSize)
	go func() {
		for msg := range msgs {
			msgIds := SendSequenceUpdate(ctx, msg, producer)
			out <- msgIds
		}
		close(out)
	}()
	return out
}

func SendSequenceUpdate(ctx context.Context, inputMsgs []*model2.PulsarEventRow, producer pulsar.Producer) []*model.ConsumerMessageId {
	offsets := make([]*armadaevents.Offset, len(inputMsgs))
	messageIds := make([]*model.ConsumerMessageId, len(inputMsgs))
	for i := 0; i < len(messageIds); i++ {
		messageIds[i] = inputMsgs[i].MessageId
		offsets[i] = &armadaevents.Offset{
			JobsetId: inputMsgs[i].Event.JobSetId,
			Offset:   inputMsgs[i].MessageId.Index,
		}
	}
	offsetsBatch := &armadaevents.Offsets{
		Offsets: offsets,
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
