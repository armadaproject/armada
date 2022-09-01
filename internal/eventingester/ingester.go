package eventingester

import (
	"context"
	"os"
	"os/signal"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-redis/redis"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/eventingester/batch"
	"github.com/G-Research/armada/internal/eventingester/configuration"
	"github.com/G-Research/armada/internal/eventingester/convert"
	"github.com/G-Research/armada/internal/eventingester/store"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {
	log := logrus.StandardLogger().WithField("service", "EventIngester")
	ctx := ctxlogrus.ToContext(createContextWithShutdown(), log)

	log.Info("Event Ingester Starting")

	rc := redis.NewUniversalClient(&config.Redis)
	defer func() {
		if err := rc.Close(); err != nil {
			log.WithError(err).Error("failed to close events Redis client")
		}
	}()
	eventDb := store.NewRedisEventStore(rc, config.EventRetentionPolicy)

	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	defer pulsarClient.Close()
	if err != nil {
		log.Errorf("Error creating pulsar client")
		panic(err)
	}

	// Receive messages and convert them to instructions
	log.Infof("Creating subscription to pulsar topic %s", config.Pulsar.JobsetEventsTopic)

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
	pulsarMsgs := pulsarutils.Receive(ctx, consumer, 0, 2*config.BatchSize, config.PulsarReceiveTimeout, config.PulsarBackoffTime)

	// Batch up messages
	batchedMsgs := batch.Batch(pulsarMsgs, config.BatchMessages, config.BatchDuration, 5, clock.RealClock{})

	// Turn the messages into event rows
	compressor, err := compress.NewZlibCompressor(config.MinMessageCompressionSize)
	if err != nil {
		log.Errorf("Error creating compressor for consumer")
		panic(err)
	}
	converter := &convert.MessageRowConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: config.BatchSize,
	}
	events := convert.Convert(ctx, batchedMsgs, 5, converter)

	// Insert into database
	maxSize := 4 * 1024 * 1024
	maxRows := 500
	inserted := store.InsertEvents(ctx, eventDb, events, 5, maxSize, maxRows)

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Send Acks
	go pulsarutils.Ack(ctx, []pulsar.Consumer{consumer}, inserted, wg)

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
