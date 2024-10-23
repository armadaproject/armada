package eventingester

import (
	"regexp"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/convert"
	"github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/internal/eventingester/model"
	"github.com/armadaproject/armada/internal/eventingester/store"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {
	log.Info("Event Ingester Starting")

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	metrics := metrics.Get()

	fatalRegexes := make([]*regexp.Regexp, len(config.FatalInsertionErrors))
	for i, str := range config.FatalInsertionErrors {
		rgx, err := regexp.Compile(str)
		if err != nil {
			log.Errorf("Error compiling regex %s", str)
			panic(err)
		}
		fatalRegexes[i] = rgx
	}

	rc := redis.NewUniversalClient(&config.Redis)
	defer func() {
		if err := rc.Close(); err != nil {
			log.WithError(err).Error("failed to close events Redis client")
		}
	}()
	eventDb := store.NewRedisEventStore(rc, config.EventRetentionPolicy, fatalRegexes, 100*time.Millisecond, 60*time.Second)

	// Turn the messages into event rows
	compressor, err := compress.NewZlibCompressor(config.MinMessageCompressionSize)
	if err != nil {
		log.Errorf("Error creating compressor for consumer")
		panic(err)
	}
	converter := convert.NewEventConverter(compressor, uint(config.MaxOutputMessageSizeBytes), metrics)

	// Start metric server
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	ingester := ingest.NewIngestionPipeline[*model.BatchUpdate, *armadaevents.EventSequence](
		config.Pulsar,
		config.Pulsar.JobsetEventsTopic,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		pulsar.Failover,
		jobsetevents.EventCounter,
		jobsetevents.MessageUnmarshaller,
		jobsetevents.BatchMerger,
		jobsetevents.BatchMetricPublisher,
		converter,
		eventDb,
		metrics,
	)
	if err := ingester.Run(app.CreateContextWithShutdown()); err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
