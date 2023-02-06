package scheduleringester

import (
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Scheduler database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config Configuration) {
	svcMetrics := metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "armada_scheduler_ingester_")

	log.Infof("Opening connection pool to postgres")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	schedulerDb := NewSchedulerDb(db, svcMetrics, 100*time.Millisecond, 60*time.Second)

	// Discard submit job messages not intended for this scheduler.
	submitJobFilter := func(event *armadaevents.EventSequence_Event) bool {
		// Discard if a SubmitJob event that doesn't target this scheduler.
		if e := event.GetSubmitJob(); e != nil && e.Scheduler != "pulsar" {
			return false
		}
		return true
	}

	compressor, err := compress.NewZlibCompressor(1024)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating  compressor"))
	}
	converter := NewInstructionConverter(svcMetrics, submitJobFilter, config.PriorityClasses, compressor)

	ingester := ingest.NewIngestionPipeline(
		config.Pulsar,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		converter,
		schedulerDb,
		config.Metrics,
		svcMetrics)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
