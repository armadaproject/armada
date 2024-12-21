package lookoutingesterv2

import (
	"regexp"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Lookout database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.LookoutIngesterV2Configuration) {
	ctx := app.CreateContextWithShutdown()
	ctx.Infof("Opening connection pool to postgres")
	m := metrics.Get()
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}

	fatalRegexes := make([]*regexp.Regexp, len(config.FatalInsertionErrors))
	for i, str := range config.FatalInsertionErrors {
		rgx, err := regexp.Compile(str)
		if err != nil {
			ctx.Errorf("Error compiling regex %s", str)
			panic(err)
		}
		fatalRegexes[i] = rgx
	}

	lookoutDb := lookoutdb.NewLookoutDb(db, fatalRegexes, m, config.MaxBackoff)

	compressor, err := compress.NewZlibCompressor(config.MinJobSpecCompressionSize)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating compressor"))
	}

	// Expose profiling endpoints if enabled.
	err = profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		ctx.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	// Start metric server
	shutdownMetricServer := common.ServeMetrics(ctx, config.MetricsPort)
	defer shutdownMetricServer()

	converter := instructions.NewInstructionConverter(m.Metrics, config.UserAnnotationPrefix, compressor)

	ingester := ingest.NewIngestionPipeline[*model.InstructionSet, *armadaevents.EventSequence](
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
		lookoutDb,
		m.Metrics,
	)

	if err := ingester.Run(ctx); err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
