package ingestion

import (
	"context"
	"os"
	"os/signal"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/eventapi/configuration"
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {

	log := logrus.StandardLogger().WithField("service", "EventIngester")
	ctx := ctxlogrus.ToContext(createContextWithShutdown(), log)

	log.Info("Event Ingester Starting")

	log.Infof("Opening connection pool to postgres")
	db, err := postgres.OpenPgxPool(config.Postgres)
	defer db.Close()
	if err != nil {
		log.Errorf("Error opening connection to postgres")
		panic(err)
	}
	eventDb := eventdb.NewEventDb(db)

	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	defer pulsarClient.Close()
	if err != nil {
		log.Errorf("Error creating pulsar client")
		panic(err)
	}

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: config.UpdateTopic,
	})
	if err != nil {
		log.Errorf("Error initialising pulsar producer")
		panic(err)
	}

	// Receive messages and convert them to instructions in parallel
	log.Infof("Creating subscriptions to pulsar topic %s", config.Pulsar.JobsetEventsTopic)

	// Create a pulsar consumer
	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            config.Pulsar.JobsetEventsTopic,
		SubscriptionName: config.SubscriptionName,
		Type:             pulsar.KeyShared,
	})
	if err != nil {
		log.Errorf("Error creating pulsar consumer")
		panic(err)
	}

	// Receive Pulsar messages on a channel
	pulsarMsgs := pulsarutils.Receive(ctx, consumer, 1, 2*config.BatchSize, config.PulsarReceiveTimeout, config.PulsarBackoffTime)

	// Batch up messages
	batchedMsgs := Batch(pulsarMsgs, config.BatchSize, config.BatchDuration, 5, clock.RealClock{})

	// Turn the messages into event rows
	compressor, err := compress.NewZlibCompressor(config.MinMessageCompressionSize)
	if err != nil {
		log.Errorf("Error creating compressor for consumer")
		panic(err)
	}
	converter := &MessageRowConverter{
		compressor: compressor,
		eventDb:    eventDb,
	}
	events := Convert(ctx, batchedMsgs, 5, converter)

	// Insert into database
	inserted := InsertEvents(ctx, eventDb, events, 5)

	// Send update
	sequenceUpdatesSent := SendSequenceUpdates(ctx, producer, inserted, 5)

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Send Acks
	go pulsarutils.Ack(ctx, []pulsar.Consumer{consumer}, sequenceUpdatesSent, wg)

	log.Info("Ingestion pipeline set up.  Running until shutdown event received")
	// wait for a shutdown event
	wg.Wait()
	log.Info("Shutdown event received- closing")
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
