package pulsarutils

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// DeadLetterMetadata describes why a message ended up on the dead-letter topic.
// It is recorded as Pulsar message properties alongside the serialized payload.
type DeadLetterMetadata struct {
	OriginalTopic string
	Subscription  string
	Attempts      int
	LastError     string
	MessageIDs    []string
}

func (m DeadLetterMetadata) properties() map[string]string {
	return map[string]string{
		"originalTopic": m.OriginalTopic,
		"subscription":  m.Subscription,
		"attempts":      strconv.Itoa(m.Attempts),
		"lastError":     m.LastError,
		"messageIds":    strings.Join(m.MessageIDs, ","),
	}
}

// DeadLetterPublisher publishes poisoned message payloads to a dead-letter Pulsar topic.
type DeadLetterPublisher struct {
	producer    pulsar.Producer
	sendTimeout time.Duration
}

func NewDeadLetterPublisher(
	client pulsar.Client,
	topic string,
	compressionType pulsar.CompressionType,
	compressionLevel pulsar.CompressionLevel,
	sendTimeout time.Duration,
) (*DeadLetterPublisher, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:              topic,
		CompressionType:    compressionType,
		CompressionLevel:   compressionLevel,
		BatcherBuilderType: pulsar.KeyBasedBatchBuilder,
		DisableBatching:    false,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &DeadLetterPublisher{
		producer:    producer,
		sendTimeout: sendTimeout,
	}, nil
}

// Publish sends payload to the dead-letter topic with meta recorded as message properties.
func (p *DeadLetterPublisher) Publish(ctx *armadacontext.Context, payload []byte, meta DeadLetterMetadata) error {
	sendCtx, cancel := armadacontext.WithTimeout(ctx, p.sendTimeout)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(1)
	var sendErr error
	p.producer.SendAsync(sendCtx, &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: meta.properties(),
	}, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
		sendErr = err
		wg.Done()
	})
	wg.Wait()
	if sendErr != nil {
		return errors.WithMessage(sendErr, "error sending message to dead-letter topic")
	}
	return nil
}

func (p *DeadLetterPublisher) Close() {
	p.producer.Close()
}
