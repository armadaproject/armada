package ingestion

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/configuration"
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {

	if !(config.Paralellism > 0) {
		panic("Lookout ingester paralellism must be greater than 0")
	}

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

	// initialise a jobsetMapper preloaded with the last day's mappings
	jobsetmapper, err := eventapi.NewJobsetMapper(eventDb, 100000, 24*time.Hour)
	if err != nil {
		log.Errorf("Error initialising jobset mapper")
		panic(err)
	}

	// Receive messages and convert them to instructions in parallel
	log.Infof("Creating %d subscriptions to pulsar topic %s", config.Paralellism, config.Pulsar.JobsetEventsTopic)
	eventChannels := make([]chan *model.PulsarEventRow, config.Paralellism)
	consumers := make([]pulsar.Consumer, config.Paralellism)
	for i := 0; i < config.Paralellism; i++ {

		// Create a pulsar consumer
		consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:            config.Pulsar.JobsetEventsTopic,
			SubscriptionName: config.SubscriptionName,
			Type:             pulsar.KeyShared,
		})
		if err != nil {
			log.Errorf("Error creating pulsar consumer %d", i)
			panic(err)
		}

		// Receive Pulsar messages on a channel
		pulsarMsgs := pulsarutils.Receive(ctx, consumer, i, 2*config.BatchSize, config.PulsarReceiveTimeout, config.PulsarBackoffTime)

		// Turn the messages into instructions
		compressor, err := compress.NewZlibCompressor(config.MinMessageCompressionSize)
		if err != nil {
			log.Errorf("Error creating compressor for consumer %d", i)
			panic(err)
		}
		converter := &MessageRowConverter{
			compressor:   compressor,
			jobsetMapper: jobsetmapper,
		}
		eventChannels[i] = Convert(ctx, pulsarMsgs, 2*config.BatchSize, converter)
		consumers[i] = consumer
	}

	// Create a merged set of msgs from each stream.  Ordering within each stream is preserved
	mergedMsgs := merge(eventChannels)

	// Batch up messages
	batchedMsgs := Batch(mergedMsgs, config.BatchSize, config.BatchDuration, 5, clock.RealClock{})

	// Insert into database
	inserted := ProcessUpdates(ctx, eventDb, batchedMsgs, 5)

	// Send update
	sequenceUpdatesSent := SendSequenceUpdates(ctx, nil, inserted, 5)

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Send Acks
	go pulsarutils.Ack(ctx, consumers, sequenceUpdatesSent, wg)

	log.Info("Ingestion pipeline set up.  Running until shutdown event received")
	// wait for a shutdown event
	wg.Wait()
	log.Info("Shutdown event received- closing")
}

// Merges an array of channels into a single channel
// TODO: This is basically the same asMergeInstructions in the lookout ingester
// Once generics arrive we should factor this out
func merge(cs []chan *model.PulsarEventRow) <-chan *model.PulsarEventRow {
	out := make(chan *model.PulsarEventRow)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan *model.PulsarEventRow) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
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
