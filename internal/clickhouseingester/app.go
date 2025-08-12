package clickhouseingester

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/clickhouseingester/clickhousedb"
	"github.com/armadaproject/armada/internal/clickhouseingester/instructions"
	"github.com/armadaproject/armada/internal/clickhouseingester/model"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update clickhouse.
// This pipeline will run until a SIGTERM is received.
func Run(config Configuration) error {

	ctx := armadacontext.Background()

	// ////////////////////////////////////////////////////////////////////////
	// Database
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("opening connection to clickhouse")
	db, err := clickhousedb.OpenClickHouse(ctx, "", "", "", "")
	if err != nil {
		return err
	}
	defer util.CloseResource("clickhouse", db)
	schedulerDb := clickhousedb.New(db)

	// ////////////////////////////////////////////////////////////////////////
	// Event Conversions
	// ////////////////////////////////////////////////////////////////////////
	instructionsConverter := instructions.NewConverter(config.UserAnnotationPrefix)

	// ////////////////////////////////////////////////////////////////////////
	// Profiling
	// ////////////////////////////////////////////////////////////////////////
	err = profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		return errors.WithMessage(err, "Pprof setup failed")
	}

	// ////////////////////////////////////////////////////////////////////////
	// Metrics
	// ////////////////////////////////////////////////////////////////////////
	ctx.Infof("setting up metrics")
	svcMetrics := metrics.NewMetrics(metrics.ArmadaClickhouseIngesterMetricsPrefix)
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	// ////////////////////////////////////////////////////////////////////////
	// Pipeline
	// ////////////////////////////////////////////////////////////////////////
	jobSetEventsIngester := ingest.NewIngestionPipeline[*model.Instructions, *armadaevents.EventSequence](
		config.Pulsar,
		config.Pulsar.JobsetEventsTopic,
		"lookout-clickhouse-ingester",
		config.BatchSize,
		config.BatchDuration,
		pulsar.Failover,
		jobsetevents.EventCounter,
		jobsetevents.MessageUnmarshaller,
		jobsetevents.BatchMerger,
		jobsetevents.BatchMetricPublisher,
		instructionsConverter,
		schedulerDb,
		svcMetrics,
	)
	return jobSetEventsIngester.Run(ctx)
}
