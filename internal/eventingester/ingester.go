package eventingester

import (
	"regexp"
	"time"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/app"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
	"github.com/armadaproject/armada/internal/eventingester/convert"
	"github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/internal/eventingester/store"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Events database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.EventIngesterConfiguration) {
	log.Info("Event Ingester Starting")

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
	converter := convert.NewEventConverter(compressor, config.BatchSize, metrics)

	ingester := ingest.
		NewIngestionPipeline(config.Pulsar, config.SubscriptionName, config.BatchSize, config.BatchDuration, converter, eventDb, config.Metrics, metrics)
	err = ingester.Run(app.CreateContextWithShutdown())

	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
