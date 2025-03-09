package pulsarutils

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	psutils "github.com/armadaproject/armada/internal/common/pulsarutils/utils"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher[T utils.ArmadaEvent] interface {
	PublishMessages(ctx *armadacontext.Context, events ...T) error
	Close()
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher[T utils.ArmadaEvent] struct {
	// Used to send messages to pulsar
	producer     pulsar.Producer
	preProcessor psutils.PreProcessor[T]
	keyRetriever psutils.KeyRetriever[T]
	// Timeout after which async messages sends will be considered failed
	sendTimeout time.Duration
}

func NewPulsarPublisher[T utils.ArmadaEvent](
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	preProcessor psutils.PreProcessor[T],
	keyRetriever psutils.KeyRetriever[T],
	sendTimeout time.Duration,
) (*PulsarPublisher[T], error) {
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PulsarPublisher[T]{
		producer:     producer,
		preProcessor: preProcessor,
		keyRetriever: keyRetriever,
		sendTimeout:  sendTimeout,
	}, nil
}

// PublishMessages publishes all event sequences to pulsar. Event sequences for a given jobset will be combined into
// single event sequences up to maxMessageBatchSize.
func (p *PulsarPublisher[T]) PublishMessages(ctx *armadacontext.Context, events ...T) error {
	processed, err := p.preProcessor(events)
	if err != nil {
		return err
	}
	msgs := make([]*pulsar.ProducerMessage, len(processed))
	for i, sequence := range processed {
		// Given we know ArmadaEvents only consist of generated proto Messages, all of which implement proto.Message
		bytes, err := proto.Marshal(any(sequence).(proto.Message))
		if err != nil {
			return err
		}
		msgs[i] = &pulsar.ProducerMessage{
			Payload: bytes,
			Key:     p.keyRetriever(sequence),
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
				ctx.Logger().
					WithStacktrace(err).
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

func (p *PulsarPublisher[T]) Close() {
	p.producer.Close()
}
