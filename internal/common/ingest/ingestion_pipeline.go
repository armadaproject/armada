package ingest

import (
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// HasPulsarMessageIds should be implemented by structs that can store a batch of pulsar message ids
// This is needed so we can pass message Ids down the pipeline and ack them at the end
type HasPulsarMessageIds interface {
	GetMessageIDs() []pulsar.MessageID
}

// InstructionConverter should be implemented by structs that can convert a batch of event sequences into an object
// suitable for passing to the sink
type InstructionConverter[T HasPulsarMessageIds] interface {
	Convert(*EventSequencesWithIds) T
}

// Sink should be implemented by the struct responsible for putting the data in its final resting place, e.g. a
// database.
type Sink[T HasPulsarMessageIds] interface {
	// Store should persist the sink.  The store is responsible for retrying failed attempts and should only return an error
	// When it is satisfied that operation cannot be retries.
	Store(T) error
}

// EventSequencesWithIds consists of a batch of Event Sequences along with the corresponding Pulsar Message Ids
type EventSequencesWithIds struct {
	EventSequences []*armadaevents.EventSequence
	MessageIds     []pulsar.MessageID
}

// IngestionPipeline is an application tha reads message from pulsar and inserts them into a sink. The pipeline will
// handle the following automatically:
//   - Receiving messages from pulsar
//   - Splitting messages into batches for efficient processing
//   - Unmarshalling into event sequences
//   - Acking processed messages
//   - Gracefully terminating on a SIGTERM
//
// Callers must supply two structs, an InstructionConverter for converting event sequences into something that can be
// exhausted and a Sink capable of exhausting these objects
type IngestionPipeline[T HasPulsarMessageIds] struct {
	pulsarConfig           configuration.PulsarConfig
	metricsConfig          configuration.MetricsConfig
	pulsarSubscriptionName string
	pulsarbatchSize        int
	pulsarbatchDuration    time.Duration
	converter              InstructionConverter[T]
	sink                   Sink[T]
}

func NewIngestionPipeline[T HasPulsarMessageIds](pulsarConfig configuration.PulsarConfig,
	pulsarSubscriptionName string,
	pulsarbatchSize int,
	pulsarbatchDuration time.Duration,
	converter InstructionConverter[T],
	sink Sink[T],
	metricsConfig configuration.MetricsConfig,
) *IngestionPipeline[T] {
	return &IngestionPipeline[T]{
		pulsarConfig:           pulsarConfig,
		metricsConfig:          metricsConfig,
		pulsarSubscriptionName: pulsarSubscriptionName,
		pulsarbatchSize:        pulsarbatchSize,
		pulsarbatchDuration:    pulsarbatchDuration,
		converter:              converter,
		sink:                   sink,
	}
}

// Run will run the ingestion pipeline until a SIGTERM is received
func (ingester *IngestionPipeline[T]) Run() {
	// create a context that will end on a sigterm
	ctx := createContextWithShutdown()

	shutdownMetricServer := common.ServeMetrics(ingester.metricsConfig.Port)
	defer shutdownMetricServer()

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Subscribe to Pulsar and receive messages
	pulsarClient, err := pulsarutils.NewPulsarClient(&ingester.pulsarConfig)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating pulsar client"))
	}
	defer pulsarClient.Close()

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       ingester.pulsarConfig.JobsetEventsTopic,
		SubscriptionName:            ingester.pulsarSubscriptionName,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	defer consumer.Close()

	if err != nil {
		panic(errors.WithMessage(err, "Error creating pulsar consumer"))
	}
	pulsarMsgs := pulsarutils.Receive(ctx, consumer, ingester.pulsarConfig.ReceiveTimeout, ingester.pulsarConfig.BackoffTime)

	// Batch up messages
	batchedMsgs := Batch(pulsarMsgs, ingester.pulsarbatchSize, ingester.pulsarbatchDuration, clock.RealClock{})

	// Convert to event sequences
	eventSequences := make(chan *EventSequencesWithIds)
	go func() {
		for msg := range batchedMsgs {
			converted := unmarshalEventSequences(msg)
			eventSequences <- converted
		}
		close(eventSequences)
	}()

	// Convert to instructions
	instructions := make(chan T)
	go func() {
		for msg := range eventSequences {
			converted := ingester.converter.Convert(msg)
			instructions <- converted
		}
		close(instructions)
	}()

	// Publish messages to sink then ACK on pulsar
	go func() {
		for msg := range instructions {
			// The sink is responsible for retrying any messages so if we get a message here we know we can give up
			// and just ACK the ids
			err := ingester.sink.Store(msg)
			log.WithError(err).Warn("Error inserting messages")
			for _, msgId := range msg.GetMessageIDs() {
				consumer.AckID(msgId)
			}
		}
		wg.Add(1)
	}()

	log.Info("Ingestion pipeline set up. Running until shutdown event received")
	// wait for a shutdown event
	wg.Wait()
	log.Info("Shutdown event received - closing")
}

// createContextWithShutdown returns a context that will report done when a SIGTERM is received
func createContextWithShutdown() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}

func unmarshalEventSequences(batch []pulsar.Message) *EventSequencesWithIds {
	sequences := make([]*armadaevents.EventSequence, 0, len(batch))
	messageIds := make([]pulsar.MessageID, len(batch))
	for i, msg := range batch {

		// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
		// As they must be acked at the end
		messageIds[i] = msg.ID()

		// If it's not a control message then ignore
		if !armadaevents.IsControlMessage(msg) {
			continue
		}

		// Try and unmarshall the proto
		es, err := eventutil.UnmarshalEventSequence(context.Background(), msg.Payload())
		if err != nil {
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
