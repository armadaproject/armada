package scheduler

import (
	"context"
	"github.com/google/uuid"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type Publisher interface {
	PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, token LeaderToken) error
	PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error)
}

type PulsarPublisher struct {
	topics              []string
	producer            pulsar.Producer
	leaderController    LeaderController
	pulsarSendTimeout   time.Duration
	maxMessageBatchSize int
}

func (p *PulsarPublisher) PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, token LeaderToken) error {
	sequences := eventutil.CompactEventSequences(events)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, p.maxMessageBatchSize, false)
	if err != nil {
		// This should never happen. We pass strict=false to the above sequence
		panic(errors.WithMessage(err, "Failed to limit sequence by size"))
	}
	msgs := make([]*pulsar.ProducerMessage, len(sequences))
	for i, sequence := range sequences {
		bytes, err := proto.Marshal(sequence)
		if err != nil {
			return err
		}
		msgs[i] = &pulsar.ProducerMessage{
			Payload: bytes,
			Properties: map[string]string{
				armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
			},
			Key: sequences[i].JobSetName,
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	// Send messages
	if p.leaderController.ValidateToken(token) {
		sendCtx, cancel := context.WithTimeout(ctx, p.pulsarSendTimeout)
		errored := false
		for _, msg := range msgs {
			p.producer.SendAsync(sendCtx, msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
					log.WithError(err).Error("error sending message to Pulsar")
					errored = true
				}
				wg.Done()
			})
		}
		wg.Wait()
		cancel()
		if errored {
			return errors.New("One or more messages failed to send to Pulsar")
		}
	}
	return nil
}

func (p *PulsarPublisher) PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	return 0, errors.New("Not implemented yet")
}
