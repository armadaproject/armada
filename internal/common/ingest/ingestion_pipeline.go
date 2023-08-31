package ingest

import (
	gocontext "context"
	"github.com/armadaproject/armada/internal/common/context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common"
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
	Convert(ctx *context.ArmadaContext, msg *EventSequencesWithIds) T
}

// Sink should be implemented by the struct responsible for putting the data in its final resting place, e.g. a
// database.
type Sink[T HasPulsarMessageIds] interface {
	// Store should persist the sink.  The store is responsible for retrying failed attempts and should only return an error
	// When it is satisfied that operation cannot be retries.
	Store(ctx *context.ArmadaContext, msg T) error
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
	metricsConfig          configuration.MetricsConfig
	metrics                *commonmetrics.Metrics
	pulsarSubscriptionName string
	pulsarBatchSize        int
	pulsarBatchDuration    time.Duration
	pulsarSubscriptionType pulsar.SubscriptionType
	msgFilter              func(msg pulsar.Message) bool
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
	metricsConfig configuration.MetricsConfig,
	metrics *commonmetrics.Metrics,
) *IngestionPipeline[T] {
	return NewFilteredMsgIngestionPipeline[T](
		pulsarConfig,
		pulsarSubscriptionName,
		pulsarBatchSize,
		pulsarBatchDuration,
		pulsarSubscriptionType,
		func(_ pulsar.Message) bool { return true },
		converter,
		sink,
		metricsConfig,
		metrics,
	)
}

// NewFilteredMsgIngestionPipeline creates an IngestionPipeline that processes only messages corresponding to the
// supplied message filter
func NewFilteredMsgIngestionPipeline[T HasPulsarMessageIds](
	pulsarConfig configuration.PulsarConfig,
	pulsarSubscriptionName string,
	pulsarBatchSize int,
	pulsarBatchDuration time.Duration,
	pulsarSubscriptionType pulsar.SubscriptionType,
	msgFilter func(msg pulsar.Message) bool,
	converter InstructionConverter[T],
	sink Sink[T],
	metricsConfig configuration.MetricsConfig,
	metrics *commonmetrics.Metrics,
) *IngestionPipeline[T] {
	return &IngestionPipeline[T]{
		pulsarConfig:           pulsarConfig,
		metricsConfig:          metricsConfig,
		metrics:                metrics,
		pulsarSubscriptionName: pulsarSubscriptionName,
		pulsarBatchSize:        pulsarBatchSize,
		pulsarBatchDuration:    pulsarBatchDuration,
		pulsarSubscriptionType: pulsarSubscriptionType,
		msgFilter:              msgFilter,
		converter:              converter,
		sink:                   sink,
	}
}

// Run will run the ingestion pipeline until the supplied context is shut down
func (ingester *IngestionPipeline[T]) Run(ctx *context.ArmadaContext) error {
	shutdownMetricServer := common.ServeMetrics(ingester.metricsConfig.Port)
	defer shutdownMetricServer()

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	if ingester.consumer == nil {
		consumer, closePulsar, err := ingester.subscribe()
		if err != nil {
			return err
		}
		ingester.consumer = consumer
		defer closePulsar()
	}
	pulsarMsgs := pulsarutils.Receive(
		ctx,
		ingester.consumer,
		ingester.pulsarConfig.ReceiveTimeout,
		ingester.pulsarConfig.BackoffTime,
		ingester.metrics)

	// Set up a context that n seconds after ctx
	// This gives the rest of the pipeline a chance to flush pending messages
	pipelineShutdownContext, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				time.Sleep(2 * ingester.pulsarBatchDuration)
				log.Infof("Waited for %v: forcing cancel", 2*ingester.pulsarBatchDuration)
				cancel()
			}
		}
	}()

	// Batch up messages
	batchedMsgs := make(chan []pulsar.Message)
	batcher := NewBatcher[pulsar.Message](pulsarMsgs, ingester.pulsarBatchSize, ingester.pulsarBatchDuration, func(b []pulsar.Message) { batchedMsgs <- b })
	go func() {
		batcher.Run(pipelineShutdownContext)
		close(batchedMsgs)
	}()

	// Convert to event sequences
	eventSequences := make(chan *EventSequencesWithIds)
	go func() {
		for msg := range batchedMsgs {
			converted := unmarshalEventSequences(msg, ingester.msgFilter, ingester.metrics)
			eventSequences <- converted
		}
		close(eventSequences)
	}()

	// Convert to instructions
	instructions := make(chan T)
	go func() {
		for msg := range eventSequences {
			converted := ingester.converter.Convert(pipelineShutdownContext, msg)
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
			err := ingester.sink.Store(pipelineShutdownContext, msg)
			taken := time.Now().Sub(start)
			if err != nil {
				log.WithError(err).Warn("Error inserting messages")
			} else {
				log.Infof("Inserted %d pulsar messages in %dms", len(msg.GetMessageIDs()), taken.Milliseconds())
			}
			if errors.Is(err, gocontext.DeadlineExceeded) {
				// This occurs when we're shutting down- it's a signal to stop processing immediately
				break
			} else {
				for _, msgId := range msg.GetMessageIDs() {
					util.RetryUntilSuccess(
						context.Background(),
						func() error { return ingester.consumer.AckID(msgId) },
						func(err error) {
							log.WithError(err).Warnf("Pulsar ack failed; backing off for %s", ingester.pulsarConfig.BackoffTime)
							time.Sleep(ingester.pulsarConfig.BackoffTime)
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

func (ingester *IngestionPipeline[T]) subscribe() (pulsar.Consumer, func(), error) {
	// Subscribe to Pulsar and receive messages
	pulsarClient, err := pulsarutils.NewPulsarClient(&ingester.pulsarConfig)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Error creating pulsar client")
	}

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       ingester.pulsarConfig.JobsetEventsTopic,
		SubscriptionName:            ingester.pulsarSubscriptionName,
		Type:                        ingester.pulsarSubscriptionType,
		ReceiverQueueSize:           ingester.pulsarConfig.ReceiverQueueSize,
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

func unmarshalEventSequences(batch []pulsar.Message, msgFilter func(msg pulsar.Message) bool, metrics *commonmetrics.Metrics) *EventSequencesWithIds {
	sequences := make([]*armadaevents.EventSequence, 0, len(batch))
	messageIds := make([]pulsar.MessageID, len(batch))
	for i, msg := range batch {

		// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
		// As they must be acked at the end
		messageIds[i] = msg.ID()

		// If we're not interested in this then continue
		if !msgFilter(msg) {
			continue
		}

		// Try and unmarshall the proto
		es, err := eventutil.UnmarshalEventSequence(context.Background(), msg.Payload())
		if err != nil {
			metrics.RecordPulsarMessageError(commonmetrics.PulsarMessageErrorDeserialization)
			log.WithError(err).Warnf("Could not unmarshal proto for msg %s", msg.ID())
			continue
		}

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
