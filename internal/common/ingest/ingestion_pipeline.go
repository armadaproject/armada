package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/util"
)

// HasPulsarMessageIds should be implemented by structs that can store a batch of pulsar message ids
// This is needed so we can pass message Ids down the pipeline and ack them at the end
type HasPulsarMessageIds interface {
	GetMessageIDs() []pulsar.MessageID
}

// EventCounter determines the true count of events, as some utils.ArmadaEvent can contain nested events
type EventCounter[T utils.ArmadaEvent] func(events *utils.EventsWithIds[T]) int

// MessageUnmarshaller converts consumed pulsar messages to the intermediate type, utils.EventsWithIds.
type MessageUnmarshaller[T utils.ArmadaEvent] func(msg pulsar.ConsumerMessage, metrics *commonmetrics.Metrics) *utils.EventsWithIds[T]

// BatchMerger merges together events within the batch, where possible
type BatchMerger[T utils.ArmadaEvent] func(batch []*utils.EventsWithIds[T]) *utils.EventsWithIds[T]

// BatchMetricPublisher logs a summary of the batching process
type BatchMetricPublisher[T utils.ArmadaEvent] func(metrics *commonmetrics.Metrics, batch *utils.EventsWithIds[T])

// InstructionConverter should be implemented by structs that can convert a batch of eventsWithIds into an object
// suitable for passing to the sink
type InstructionConverter[T HasPulsarMessageIds, U utils.ArmadaEvent] interface {
	Convert(ctx *armadacontext.Context, msg *utils.EventsWithIds[U]) T
}

// Sink should be implemented by the struct responsible for putting the data in its final resting place, e.g. a
// database.
type Sink[T HasPulsarMessageIds] interface {
	// Store should persist the sink.  The store is responsible for retrying failed attempts and should only return an error
	// When it is satisfied that operation cannot be retries.
	Store(ctx *armadacontext.Context, msg T) error
}

// IngestionPipeline is a pipeline that reads message from pulsar and inserts them into a sink. The pipeline will
// handle the following automatically:
//   - Receiving messages from pulsar
//   - Unmarshalling into eventsWithIds
//   - Combining messages into batches for efficient processing
//   - Publishing relevant metrics related to batch
//   - Converting eventsWithIds to instructions
//   - Acking processed messages
//
// Callers must supply two structs, an InstructionConverter for converting eventsWithIds into something that can be
// exhausted and a Sink capable of exhausting these objects
type IngestionPipeline[T HasPulsarMessageIds, U utils.ArmadaEvent] struct {
	pulsarConfig           commonconfig.PulsarConfig
	metrics                *commonmetrics.Metrics
	pulsarTopic            string
	pulsarSubscriptionName string
	pulsarBatchSize        int
	pulsarBatchDuration    time.Duration
	pulsarSubscriptionType pulsar.SubscriptionType
	eventCounter           EventCounter[U]
	messageConverter       MessageUnmarshaller[U]
	batchMerger            BatchMerger[U]
	metricPublisher        BatchMetricPublisher[U]
	converter              InstructionConverter[T, U]
	sink                   Sink[T]
	consumer               pulsar.Consumer // for test purposes only
}

// NewIngestionPipeline creates an IngestionPipeline that processes all pulsar messages
func NewIngestionPipeline[T HasPulsarMessageIds, U utils.ArmadaEvent](
	pulsarConfig commonconfig.PulsarConfig,
	pulsarTopic string,
	pulsarSubscriptionName string,
	pulsarBatchSize int,
	pulsarBatchDuration time.Duration,
	pulsarSubscriptionType pulsar.SubscriptionType,
	eventCounter EventCounter[U],
	messageConverter MessageUnmarshaller[U],
	batchMerger BatchMerger[U],
	metricPublisher BatchMetricPublisher[U],
	converter InstructionConverter[T, U],
	sink Sink[T],
	metrics *commonmetrics.Metrics,
) *IngestionPipeline[T, U] {
	return &IngestionPipeline[T, U]{
		pulsarConfig:           pulsarConfig,
		pulsarTopic:            pulsarTopic,
		metrics:                metrics,
		pulsarSubscriptionName: pulsarSubscriptionName,
		pulsarBatchSize:        pulsarBatchSize,
		pulsarBatchDuration:    pulsarBatchDuration,
		pulsarSubscriptionType: pulsarSubscriptionType,
		eventCounter:           eventCounter,
		messageConverter:       messageConverter,
		batchMerger:            batchMerger,
		metricPublisher:        metricPublisher,
		converter:              converter,
		sink:                   sink,
	}
}

// Run will run the ingestion pipeline until the supplied context is shut down
func (i *IngestionPipeline[T, U]) Run(ctx *armadacontext.Context) error {
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
	pulsarMessageChannel := i.consumer.Chan()
	pulsarMessages := make(chan pulsar.ConsumerMessage)

	// Consume pulsar messages
	// Used to track if we are no longer receiving pulsar messages
	go func() {
		timeout := time.Minute * 2
		ticker := time.NewTicker(timeout)
		lastReceivedTime := time.Now()
	loop:
		for {
			select {
			case msg, ok := <-pulsarMessageChannel:
				if !ok {
					// Channel closed
					break loop
				}
				pulsarMessages <- msg
				lastReceivedTime = time.Now()
			case <-ticker.C:
				timeSinceLastReceived := time.Now().Sub(lastReceivedTime)
				if timeSinceLastReceived > timeout {
					ctx.Infof("%s - Last pulsar message received %s ago", i.pulsarTopic, timeSinceLastReceived)
				}
			}
		}
		close(pulsarMessages)
	}()

	// Convert to eventsWithIds
	events := make(chan *utils.EventsWithIds[U])
	go func() {
		for msg := range pulsarMessages {
			converted := i.messageConverter(msg, i.metrics)
			events <- converted
		}
		close(events)
	}()

	// Batch up messages
	batchedEvents := make(chan []*utils.EventsWithIds[U])
	batcher := NewBatcher[*utils.EventsWithIds[U]](events, i.pulsarBatchSize, i.pulsarBatchDuration, i.eventCounter, batchedEvents)
	go func() {
		batcher.Run(ctx)
		close(batchedEvents)
	}()

	// Merge intermediate event batches
	mergedEventBatches := make(chan *utils.EventsWithIds[U])
	go func() {
		for batch := range batchedEvents {
			mergedEventBatches <- i.batchMerger(batch)
		}
		close(mergedEventBatches)
	}()

	// Log summary of batch
	preprocessedEventBatches := make(chan *utils.EventsWithIds[U])
	go func() {
		for batch := range mergedEventBatches {
			i.metricPublisher(i.metrics, batch)
			preprocessedEventBatches <- batch
		}
		close(preprocessedEventBatches)
	}()

	// Convert to instructions
	instructions := make(chan T)
	go func() {
		for batch := range preprocessedEventBatches {
			start := time.Now()
			converted := i.converter.Convert(ctx, batch)
			taken := time.Now().Sub(start)
			ctx.Infof("%s - Processed %d pulsar messages in %dms", i.pulsarTopic, len(batch.MessageIds), taken.Milliseconds())
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
				ctx.Logger.WithError(err).Warnf("%s - Error inserting messages", i.pulsarTopic)
			} else {
				ctx.Infof("%s - Inserted %d pulsar messages in %dms", i.pulsarTopic, len(msg.GetMessageIDs()), taken.Milliseconds())
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
							ctx.Logger.WithError(err).Warnf("%s - Pulsar ack failed; backing off for %s", i.pulsarTopic, i.pulsarConfig.BackoffTime)
							time.Sleep(i.pulsarConfig.BackoffTime)
						},
					)
					i.metrics.RecordPulsarMessageProcessed()
				}
			}
		}
		wg.Done()
	}()

	ctx.Infof("%s - Ingestion pipeline set up. Running until shutdown event received", i.pulsarTopic)
	// wait for a shutdown event
	wg.Wait()
	ctx.Infof("%s - Shutdown event received - closing", i.pulsarTopic)
	return nil
}

func (i *IngestionPipeline[T, U]) subscribe() (pulsar.Consumer, func(), error) {
	// Subscribe to Pulsar and receive messages
	pulsarClient, err := pulsarutils.NewPulsarClient(&i.pulsarConfig)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "Error creating pulsar client")
	}

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       i.pulsarTopic,
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
