package scheduleringester

import (
	"github.com/G-Research/armada/internal/common/app"
	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/eventingester/metrics"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/scheduleringester/configuration"
	"github.com/G-Research/armada/internal/scheduleringester/instructions"
	"github.com/G-Research/armada/internal/scheduleringester/schedulerdb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Schedulerdb database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.SchedulerIngesterConfiguration) {
	log.Infof("Opening connection pool to postgres")
	metrics := metrics.Get()
	db, err := postgres.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	schedulerDb := schedulerdb.NewSchedulerDb(db, metrics)

	converter := instructions.NewInstructionConverter(metrics)

	ingester := ingest.NewIngestionPipeline(
		config.Pulsar,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		converter,
		schedulerDb,
		config.Metrics,
		metrics)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
