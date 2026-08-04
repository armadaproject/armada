package ingest

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
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
	// Serialize renders msg as a self-contained byte payload for the dead-letter topic.
	// Only called when the pipeline is about to give up on msg, never on the happy path.
	Serialize(msg T) ([]byte, error)
}

// deadLetterPublisher is implemented by *pulsarutils.DeadLetterPublisher; declared here so tests can inject a fake.
type deadLetterPublisher interface {
	Publish(ctx *armadacontext.Context, payload []byte, meta pulsarutils.DeadLetterMetadata) error
	Close()
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
	deadLetterPublisher    deadLetterPublisher
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

// defaultDeadLetterMaxAttempts is used when PulsarConfig.DeadLetterMaxAttempts is unset (0),
// i.e. dead-lettering has not been explicitly configured. It still bounds retries so that a
// persistently failing message doesn't block the pipeline forever; the message is dead-lettered
// (if DeadLetterTopic is set) or, if not, left unacked for redelivery - see deadLetterMaxAttempts.
const defaultDeadLetterMaxAttempts = 5

// deadLetterMaxAttempts returns the effective number of Sink.Store attempts before a message is
// given up on, defaulting to defaultDeadLetterMaxAttempts when PulsarConfig.DeadLetterMaxAttempts
// is unset.
func (i *IngestionPipeline[T, U]) deadLetterMaxAttempts() int {
	if i.pulsarConfig.DeadLetterMaxAttempts > 0 {
		return i.pulsarConfig.DeadLetterMaxAttempts
	}
	return defaultDeadLetterMaxAttempts
}

// newBackOff returns a fresh jittered exponential backoff sequence, starting at
// i.pulsarConfig.BackoffTime and capped at i.pulsarConfig.MaxBackoffTime (falling back to
// BackoffTime, i.e. no growth, if MaxBackoffTime is unset). Randomization and growth can be
// tuned via i.pulsarConfig.BackoffRandomizationFactor and BackoffMultiplier; if unset, the
// backoff library's own defaults are used.
func (i *IngestionPipeline[T, U]) newBackOff() *backoff.ExponentialBackOff {
	maxInterval := i.pulsarConfig.MaxBackoffTime
	if maxInterval <= 0 {
		maxInterval = i.pulsarConfig.BackoffTime
	}
	opts := []backoff.ExponentialBackOffOpts{
		backoff.WithInitialInterval(i.pulsarConfig.BackoffTime),
		backoff.WithMaxInterval(maxInterval),
		backoff.WithMaxElapsedTime(0),
	}
	if i.pulsarConfig.BackoffRandomizationFactor >= 0 {
		opts = append(opts, backoff.WithRandomizationFactor(i.pulsarConfig.BackoffRandomizationFactor))
	}
	if i.pulsarConfig.BackoffMultiplier > 0 {
		opts = append(opts, backoff.WithMultiplier(i.pulsarConfig.BackoffMultiplier))
	}
	return backoff.NewExponentialBackOff(opts...)
}

// Run will run the ingestion pipeline until the supplied context is shut down
func (i *IngestionPipeline[T, U]) Run(ctx *armadacontext.Context) error {
	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	if i.consumer == nil {
		client, consumer, closePulsar, err := i.subscribe()
		if err != nil {
			return err
		}
		i.consumer = consumer
		defer closePulsar()

		if i.pulsarConfig.DeadLetterTopic != "" {
			deadLetterPublisher, err := pulsarutils.NewDeadLetterPublisher(
				client,
				i.pulsarConfig.DeadLetterTopic,
				i.pulsarConfig.CompressionType,
				i.pulsarConfig.CompressionLevel,
				i.pulsarConfig.SendTimeout,
			)
			if err != nil {
				return errors.WithMessage(err, "error creating dead-letter publisher")
			}
			i.deadLetterPublisher = deadLetterPublisher
			defer deadLetterPublisher.Close()
		}

		if i.pulsarConfig.DelayMonitor.Enabled {
			err := i.startProcessingDelayMonitor(ctx, client)
			if err != nil {
				return errors.WithMessage(err, "error starting topic delay monitoring")
			}
		}
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
				i.metrics.RecordPulsarMessagePublishTime(i.pulsarSubscriptionName, int(msg.ID().PartitionIdx()), msg.PublishTime())
				pulsarMessages <- msg
				lastReceivedTime = time.Now()
			case <-ticker.C:
				timeSinceLastReceived := time.Since(lastReceivedTime)
				if timeSinceLastReceived > timeout {
					log.Infof("%s - Last pulsar message received %s ago", i.pulsarTopic, timeSinceLastReceived)
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
			taken := time.Since(start)
			log.Infof("%s - Processed %d pulsar messages in %dms", i.pulsarTopic, len(batch.MessageIds), taken.Milliseconds())
			instructions <- converted
		}
		close(instructions)
	}()

	// Publish messages to sink then ACK on pulsar
	go func() {
	loop:
		for msg := range instructions {
			start := time.Now()
			storeBackoff := i.newBackOff()
			dropped := false
			succeeded := util.RetryUntilSuccessOrExhausted(
				ctx,
				i.deadLetterMaxAttempts(),
				func() error {
					return i.sink.Store(ctx, msg)
				},
				func(attempt int, err error) {
					i.metrics.RecordPulsarMessageStoreRetry()
					wait := storeBackoff.NextBackOff()
					log.WithError(err).Warnf("%s - Error inserting %d messages (ids: %v); will retry after %s",
						i.pulsarTopic, len(msg.GetMessageIDs()), msg.GetMessageIDs(), wait)
					// This sleep is not ctx-aware: RetryUntilSuccessOrExhausted only checks ctx
					// before the next performAction call, not during this wait. On shutdown, the
					// sleep runs to completion before cancellation is noticed, delaying shutdown
					// by up to `wait`. Deemed acceptable since wait is bounded by backoff config.
					time.Sleep(wait)
				},
				func(lastErr error) {
					if i.deadLetterPublisher == nil {
						// No dead-letter topic configured: leave the message unacked for
						// redelivery rather than dropping it silently.
						dropped = true
						log.WithError(lastErr).Warnf("%s - Exhausted %d attempts inserting %d messages (ids: %v); no dead-letter topic configured, leaving unacked for redelivery",
							i.pulsarTopic, i.deadLetterMaxAttempts(), len(msg.GetMessageIDs()), msg.GetMessageIDs())
						return
					}
					i.metrics.RecordPulsarMessageDeadLettered()
					log.WithError(lastErr).Warnf("%s - Exhausted %d attempts inserting %d messages (ids: %v); publishing to dead-letter topic",
						i.pulsarTopic, i.deadLetterMaxAttempts(), len(msg.GetMessageIDs()), msg.GetMessageIDs())
					payload, err := i.sink.Serialize(msg)
					if err != nil {
						log.WithError(err).Warnf("%s - Error serializing dead-lettered messages (ids: %v); publishing error text instead",
							i.pulsarTopic, msg.GetMessageIDs())
						payload = []byte(err.Error())
					}
					meta := pulsarutils.DeadLetterMetadata{
						OriginalTopic: i.pulsarTopic,
						Subscription:  i.pulsarSubscriptionName,
						Attempts:      i.deadLetterMaxAttempts(),
						LastError:     lastErr.Error(),
						MessageIDs:    pulsarutils.MessageIdsToStrings(msg.GetMessageIDs()),
					}
					dlqBackoff := i.newBackOff()
					util.RetryUntilSuccess(
						ctx,
						func() error { return i.deadLetterPublisher.Publish(ctx, payload, meta) },
						func(err error) {
							wait := dlqBackoff.NextBackOff()
							log.WithError(err).Warnf("%s - Dead-letter publish failed; backing off for %s", i.pulsarTopic, wait)
							time.Sleep(wait)
						},
					)
				},
			)
			if !succeeded && (ctx.Err() != nil || dropped) {
				// Either ctx was cancelled (e.g. ingester shutdown) while retrying, or attempts
				// were exhausted with no dead-letter topic configured; the message is left
				// unacked for redelivery rather than dropped.
				break loop
			}
			taken := time.Since(start)
			log.Infof("%s - Inserted %d pulsar messages in %dms", i.pulsarTopic, len(msg.GetMessageIDs()), taken.Milliseconds())
			for _, msgId := range msg.GetMessageIDs() {
				util.RetryUntilSuccess(
					armadacontext.Background(),
					func() error { return i.consumer.AckID(msgId) },
					func(err error) {
						log.WithError(err).Warnf("%s - Pulsar ack failed; backing off for %s", i.pulsarTopic, i.pulsarConfig.BackoffTime)
						time.Sleep(i.pulsarConfig.BackoffTime)
					},
				)
				i.metrics.RecordPulsarMessageProcessed()
			}
		}
		wg.Done()
	}()

	log.Infof("%s - Ingestion pipeline set up. Running until shutdown event received", i.pulsarTopic)
	// wait for a shutdown event
	wg.Wait()
	log.Infof("%s - Shutdown event received - closing", i.pulsarTopic)
	return nil
}

func (i *IngestionPipeline[T, U]) startProcessingDelayMonitor(ctx *armadacontext.Context, pulsarClient pulsar.Client) error {
	if i.pulsarConfig.RestURL == "" {
		return fmt.Errorf("cannot enable topic delay monitoring as pulsar RestURL not configured")
	}
	pulsarAdminClient, err := pulsarutils.NewPulsarAdminClient(&i.pulsarConfig)
	if err != nil {
		return errors.WithMessage(err, "error creating pulsar admin client")
	}

	topicDelayMonitor := NewTopicProcessingDelayMonitor(pulsarClient, pulsarAdminClient, i.pulsarTopic, i.pulsarSubscriptionName, i.pulsarConfig.DelayMonitor.Interval, i.metrics)
	err = topicDelayMonitor.Initialise(ctx)
	if err != nil {
		return errors.WithMessage(err, "failed to initialise topic delay monitor")
	}
	go func() {
		log.Infof("starting topic delay monitor")
		err = topicDelayMonitor.Run(ctx)
		if err != nil {
			log.Errorf("topic delay monitor stopped with error %s", err)
		} else {
			log.Infof("topic delay monitor stopped")
		}
	}()

	return nil
}

func (i *IngestionPipeline[T, U]) subscribe() (pulsar.Client, pulsar.Consumer, func(), error) {
	// Subscribe to Pulsar and receive messages
	pulsarClient, err := pulsarutils.NewPulsarClient(&i.pulsarConfig)
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "error creating pulsar client")
	}

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       i.pulsarTopic,
		SubscriptionName:            i.pulsarSubscriptionName,
		Type:                        i.pulsarSubscriptionType,
		ReceiverQueueSize:           i.pulsarConfig.ReceiverQueueSize,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		return nil, nil, nil, errors.WithMessage(err, "error creating pulsar consumer")
	}

	return pulsarClient, consumer, func() {
		consumer.Close()
		pulsarClient.Close()
	}, nil
}
