package jobsetevents

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	PublishMessages(ctx *armadacontext.Context, events ...*armadaevents.EventSequence) error
	Close()
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher struct {
	// Used to send messages to pulsar
	producer pulsar.Producer
	// Maximum number of Events in each EventSequence
	maxEventsPerMessage int
	// Maximum size (in bytes) of produced pulsar messages.
	// This must be below 4MB which is the pulsar message size limit
	maxAllowedMessageSize uint
	// Timeout after which async messages sends will be considered failed
	sendTimeout time.Duration
}

func NewPulsarPublisher(
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	maxEventsPerMessage int,
	maxAllowedMessageSize uint,
	sendTimeout time.Duration,
) (*PulsarPublisher, error) {
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PulsarPublisher{
		producer:              producer,
		maxEventsPerMessage:   maxEventsPerMessage,
		maxAllowedMessageSize: maxAllowedMessageSize,
		sendTimeout:           sendTimeout,
	}, nil
}

// PublishMessages publishes all event sequences to pulsar. Event sequences for a given jobset will be combined into
// single event sequences up to maxMessageBatchSize.
func (p *PulsarPublisher) PublishMessages(ctx *armadacontext.Context, events ...*armadaevents.EventSequence) error {
	sequences := eventutil.CompactJobSetEventSequences(events)
	sequences = eventutil.LimitJobSetSequencesEventMessageCount(sequences, p.maxEventsPerMessage)
	sequences, err := eventutil.LimitJobSetEventSequencesByteSize(sequences, p.maxAllowedMessageSize, true)
	if err != nil {
		return err
	}
	msgs := make([]*pulsar.ProducerMessage, len(sequences))
	for i, sequence := range sequences {
		bytes, err := proto.Marshal(sequence)
		if err != nil {
			return err
		}
		msgs[i] = &pulsar.ProducerMessage{
			Payload: bytes,
			Key:     sequences[i].JobSetName,
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	// Send messages
	sendCtx, cancel := armadacontext.WithTimeout(ctx, p.sendTimeout)
	errored := false
	for _, msg := range msgs {
		p.producer.SendAsync(sendCtx, msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
			if err != nil {
				logging.
					WithStacktrace(ctx, err).
					Error("error sending message to Pulsar")
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

	return nil
}

func (p *PulsarPublisher) Close() {
	p.producer.Close()
}
