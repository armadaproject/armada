package lookoutingesterv2

import (
	"os"
	"os/signal"
	"sync"

	"github.com/G-Research/armada/internal/lookoutingesterv2/configuration"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/lookoutingesterv2/batch"
	"github.com/G-Research/armada/internal/lookoutingesterv2/instructions"
	"github.com/G-Research/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/G-Research/armada/internal/lookoutingesterv2/metrics"
	"github.com/G-Research/armada/internal/lookoutingesterv2/model"
	"github.com/G-Research/armada/internal/pulsarutils"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Lookout database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.LookoutIngesterV2Configuration) {
	if !(config.Paralellism > 0) {
		panic("Lookout ingester paralellism must be greater than 0")
	}

	log := logrus.StandardLogger().WithField("service", "PulsarIngester")
	ctx := ctxlogrus.ToContext(createContextWithShutdown(), log)

	log.Info("Lookout Ingester Starting")

	log.Infof("Opening connection pool to postgres")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		log.Errorf("Error opening connection to postgres")
		panic(err)
	}

	pulsarClient, err := pulsarutils.NewPulsarClient(&config.Pulsar)
	if err != nil {
		log.Errorf("Error creating pulsar client")
		panic(err)
	}

	// Receive messages and convert them to instructions in parallel
	log.Infof("Creating %d subscriptions to pulsar topic %s", config.Paralellism, config.Pulsar.JobsetEventsTopic)
	instructionChannels := make([]chan *model.InstructionSet, config.Paralellism)
	consumers := make([]pulsar.Consumer, config.Paralellism)
	for i := 0; i < config.Paralellism; i++ {
		// Create a pulsar consumer
		consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
			Topic:                       config.Pulsar.JobsetEventsTopic,
			SubscriptionName:            config.SubscriptionName,
			Type:                        pulsar.KeyShared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		})
		if err != nil {
			log.Errorf("Error creating pulsar consumer %d", i)
			panic(err)
		}

		// Receive Pulsar messages on a channel
		pulsarMsgs := pulsarutils.Receive(ctx, consumer, i, 2*config.BatchSize, config.PulsarReceiveTimeout, config.PulsarBackoffTime)

		// Turn the messages into instructions
		compressor, err := compress.NewZlibCompressor(config.MinJobSpecCompressionSize)
		if err != nil {
			log.Errorf("Error creating compressor for consumer %d", i)
			panic(err)
		}

		instructionsService := instructions.New(metrics.Get())
		instructionChannels[i] = instructionsService.Convert(ctx, pulsarMsgs, 2*config.BatchSize, config.UserAnnotationPrefix, compressor)
		consumers[i] = consumer
	}

	// Create a merged set of instructions from each stream.  Ordering within each stream is preserved
	mergedInstructionsCh := mergeInstructions(instructionChannels)

	// Batch up the updates.  This will release a batch of updates after batchSize or batchDuration has been reached
	// (whichever happens first)
	batchedInstructions := batch.Batch(mergedInstructionsCh, config.BatchSize, config.BatchDuration, 5, clock.RealClock{})

	// Insert the instructions into a db
	ldb := lookoutdb.New(db, metrics.Get(), 10, 60)
	acks := ldb.ProcessUpdates(ctx, batchedInstructions, 5)

	// Waitgroup that wil fire when the pipeline has been torn down
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Ack the messages - we pass a waitgroup here that will tell us when the pipeline has shutdown
	go pulsarutils.Ack(ctx, consumers, acks, &wg)

	shutdownMetricServer := common.ServeMetrics(config.Metrics.Port)
	defer shutdownMetricServer()

	log.Info("Ingestion pipeline set up. Running until shutdown event received")
	// wait for a shutdown event
	wg.Wait()
	log.Info("Shutdown event received - closing")
}

// mergeInstructions takes an array of channels and merges them into a single channel
func mergeInstructions(cs []chan *model.InstructionSet) <-chan *model.InstructionSet {
	out := make(chan *model.InstructionSet)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan *model.InstructionSet) {
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
