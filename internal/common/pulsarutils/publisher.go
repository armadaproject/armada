package pulsarutils

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	PublishMessages(ctx *armadacontext.Context, es *armadaevents.EventSequence) error
	Close()
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher struct {
	// Used to send messages to pulsar
	producer pulsar.Producer
	// Maximum size (in bytes) of produced pulsar messages.
	// This must be below 4MB which is the pulsar message size limit
	maxAllowedMessageSize uint
}

func NewPulsarPublisher(
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	maxAllowedMessageSize uint,
) (*PulsarPublisher, error) {
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PulsarPublisher{
		producer:              producer,
		maxAllowedMessageSize: maxAllowedMessageSize,
	}, nil
}

// PublishMessages publishes all event sequences to pulsar. Event sequences for a given jobset will be combined into
// single event sequences up to maxMessageBatchSize.
func (p *PulsarPublisher) PublishMessages(ctx *armadacontext.Context, es *armadaevents.EventSequence) error {
	return CompactAndPublishSequences(
		ctx,
		[]*armadaevents.EventSequence{es},
		p.producer,
		p.maxAllowedMessageSize)
}

func (p *PulsarPublisher) Close() {
	p.producer.Close()
}
