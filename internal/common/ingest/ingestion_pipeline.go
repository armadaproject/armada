package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// HasPulsarMessageIds should be implemented by structs that can store a batch of pulsar message ids
// This is needed so we can pass message Ids down the pipeline and ack them at the end
type HasPulsarMessageIds interface {
	GetMessageIDs() []pulsar.MessageID
}

// InstructionConverter should be implemented by structs that can convert a batch of event sequences into an object
// suitable for passing to the sink
type InstructionConverter[T HasPulsarMessageIds] interface {
	Convert(ctx *armadacontext.Context, msg *EventSequencesWithIds) T
}

// Sink should be implemented by the struct responsible for putting the data in its final resting place, e.g. a
// database.
type Sink[T HasPulsarMessageIds] interface {
	// Store should persist the sink.  The store is responsible for retrying failed attempts and should only return an error
	// When it is satisfied that operation cannot be retries.
	Store(ctx *armadacontext.Context, msg T) error
}

// EventSequencesWithIds consists of a batch of Event Sequences along with the corresponding Pulsar Message Ids
type EventSequencesWithIds struct {
	EventSequences []*armadaevents.EventSequence
	MessageIds     []pulsar.MessageID
}

// IngestionPipeline is a pipeline that reads message from pulsar and inserts them into a sink. The pipeline will
// handle the following automatically:
//   - Receiving messages from pulsar
//   - Combining messages into batches for efficient processing
//   - Unmarshalling into event sequences
//   - Acking processed messages
//
// Callers must supply two structs, an InstructionConverter for converting event sequences into something that can be
// exhausted and a Sink capable of exhausting these objects
type IngestionPipeline[T HasPulsarMessageIds] struct {
	pulsarConfig           configuration.PulsarConfig
	metricsPort            uint16
	metrics                *commonmetrics.Metrics
	pulsarSubscriptionName string
	pulsarBatchSize        int
	pulsarBatchDuration    time.Duration
	pulsarSubscriptionType pulsar.SubscriptionType
	converter              InstructionConverter[T]
	sink                   Sink[T]
	consumer               pulsar.Consumer // for test purposes only
}

// NewIngestionPipeline creates an IngestionPipeline that processes all pulsar messages
func NewIngestionPipeline[T HasPulsarMessageIds](
	pulsarConfig configuration.PulsarConfig,
	pulsarSubscriptionName string,
	pulsarBatchSize int,
	pulsarBatchDuration time.Duration,
	pulsarSubscriptionType pulsar.SubscriptionType,
	converter InstructionConverter[T],
	sink Sink[T],
	metricsPort uint16,
	metrics *commonmetrics.Metrics,
) *IngestionPipeline[T] {
	return &IngestionPipeline[T]{
		pulsarConfig:           pulsarConfig,
		metricsPort:            metricsPort,
		metrics:                metrics,
		pulsarSubscriptionName: pulsarSubscriptionName,
		pulsarBatchSize:        pulsarBatchSize,
		pulsarBatchDuration:    pulsarBatchDuration,
		pulsarSubscriptionType: pulsarSubscriptionType,
		converter:              converter,
		sink:                   sink,
	}
}

// Run will run the ingestion pipeline until the supplied context is shut down
func (i *IngestionPipeline[T]) Run(ctx *armadacontext.Context) error {
	shutdownMetricServer := common.ServeMetrics(i.metricsPort)
	defer shutdownMetricServer()

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	if i.consumer == nil {
		consumer, closePulsar, err := i.subscribe()
		if err != nil {
			return err
		}
		i.consumer = consumer
		defer closePulsar()
	}
	pulsarMsgs := i.consumer.Chan()

	// Convert to event sequences
	eventSequences := make(chan *EventSequencesWithIds)
	go func() {
		for msg := range pulsarMsgs {
			converted := unmarshalEventSequences(msg, i.metrics)
			eventSequences <- converted
		}
		close(eventSequences)
	}()

	// Batched up messages
	batchedEventSequences := make(chan *EventSequencesWithIds)
	eventCounterFunc := func(seq *EventSequencesWithIds) int { return len(seq.EventSequences) }
	eventPublisherFunc := func(b []*EventSequencesWithIds) { batchedEventSequences <- combineEventSequences(b) }
	batcher := NewBatcher[*EventSequencesWithIds](eventSequences, i.pulsarBatchSize, i.pulsarBatchDuration, eventCounterFunc, eventPublisherFunc)
	go func() {
		batcher.Run(ctx)
		close(batchedEventSequences)
	}()

	// Convert to instructions
	instructions := make(chan T)
	go func() {
		for msg := range batchedEventSequences {
			converted := i.converter.Convert(ctx, msg)
			instructions <- converted
		}
		close(instructions)
	}()

	// Publish messages to sink then ACK on pulsar
	go func() {
		for msg := range instructions {
			// The sink is responsible for retrying any messages so if we get a message here we know we can give up
			// and just ACK the ids
			start := time.Now()
			err := i.sink.Store(ctx, msg)
			taken := time.Now().Sub(start)
			if err != nil {
				log.WithError(err).Warn("Error inserting messages")
			} else {
				log.Infof("Inserted %d pulsar messages in %dms", len(msg.GetMessageIDs()), taken.Milliseconds())
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// This occurs when we're shutting down- it's a signal to stop processing immediately
				break
			} else {
				for _, msgId := range msg.GetMessageIDs() {
					util.RetryUntilSuccess(
						armadacontext.Background(),
						func() error { return i.consumer.AckID(msgId) },
						func(err error) {
							log.WithError(err).Warnf("Pulsar ack failed; backing off for %s", i.pulsarConfig.BackoffTime)
							time.Sleep(i.pulsarConfig.BackoffTime)
						},
					)
				}
			}
		}
		wg.Done()
	}()

	log.Info("Ingestion pipeline set up. Running until shutdown event received")
	// wait for a shutdown event
	wg.Wait()
	log.Info("Shutdown event received - closing")
	return nil
}

func (i *IngestionPipeline[T]) subscribe() (pulsar.Consumer, func(), error) {
	// Subscribe to Pulsar and receive messages
	pulsarClient, err := pulsarutils.NewPulsarClient(&i.pulsarConfig)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Error creating pulsar client")
	}

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       i.pulsarConfig.JobsetEventsTopic,
		SubscriptionName:            i.pulsarSubscriptionName,
		Type:                        i.pulsarSubscriptionType,
		ReceiverQueueSize:           i.pulsarConfig.ReceiverQueueSize,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Error creating pulsar consumer")
	}

	return consumer, func() {
		consumer.Close()
		pulsarClient.Close()
	}, nil
}
func unmarshalEventSequences(msg pulsar.ConsumerMessage, metrics *commonmetrics.Metrics) *EventSequencesWithIds {
	sequences := make([]*armadaevents.EventSequence, 0, 1)
	messageIds := make([]pulsar.MessageID, 0, 1)

	// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
	// As they must be acked at the end
	messageIds = append(messageIds, msg.ID())

	// Try and unmarshall the proto
	es, err := eventutil.UnmarshalEventSequence(armadacontext.Background(), msg.Payload())
	if err != nil {
		metrics.RecordPulsarMessageError(commonmetrics.PulsarMessageErrorDeserialization)
		log.WithError(err).Warnf("Could not unmarshal proto for msg %s", msg.ID())
	} else {
		// Fill in time if it is not set
		// TODO - once created is set everywhere we can remove this
		for _, event := range es.Events {
			if event.GetCreated() == nil {
				publishTime := msg.PublishTime()
				event.Created = &publishTime
			}
		}
		sequences = append(sequences, es)
	}
	return &EventSequencesWithIds{
		EventSequences: sequences, MessageIds: messageIds,
	}
}

func combineEventSequences(sequences []*EventSequencesWithIds) *EventSequencesWithIds {
	combinedSequences := make([]*armadaevents.EventSequence, 0)
	messageIds := []pulsar.MessageID{}
	for _, seq := range sequences {
		combinedSequences = append(combinedSequences, seq.EventSequences...)
		messageIds = append(messageIds, seq.MessageIds...)
	}
	return &EventSequencesWithIds{
		EventSequences: combinedSequences, MessageIds: messageIds,
	}
}
