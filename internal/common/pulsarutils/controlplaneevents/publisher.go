package controlplaneevents

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	PublishMessages(ctx *armadacontext.Context, events ...*controlplaneevents.ControlPlaneEventV1) error
	Close()
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher struct {
	// Used to send messages to pulsar
	producer pulsar.Producer
	// Maximum size (in bytes) of produced pulsar messages.
	// This must be below 4MB which is the pulsar message size limit
	maxAllowedMessageSize uint
	// Timeout after which async messages sends will be considered failed
	sendTimeout time.Duration
}

func NewPulsarPublisher(
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	maxAllowedMessageSize uint,
	sendTimeout time.Duration,
) (*PulsarPublisher, error) {
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PulsarPublisher{
		producer:              producer,
		maxAllowedMessageSize: maxAllowedMessageSize,
		sendTimeout:           sendTimeout,
	}, nil
}

// PublishMessages publishes control plane event sequences to pulsar.
func (p *PulsarPublisher) PublishMessages(ctx *armadacontext.Context, events ...*controlplaneevents.ControlPlaneEventV1) error {
	err := eventutil.ValidateControlPlaneEventsByteSize(events, p.maxAllowedMessageSize)
	if err != nil {
		return err
	}
	msgs := make([]*pulsar.ProducerMessage, len(events))
	for i, event := range events {
		bytes, err := proto.Marshal(event)
		if err != nil {
			return err
		}

		messageKey, err := eventutil.MessageKeyFromControlPlaneEvent(event)
		if err != nil {
			return err
		}
		msgs[i] = &pulsar.ProducerMessage{
			Payload: bytes,
			Key:     messageKey,
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
