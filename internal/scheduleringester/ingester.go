package scheduleringester

import (
	"time"

	"github.com/G-Research/armada/internal/common/database"

	"github.com/G-Research/armada/internal/common/ingest/metrics"
	"github.com/G-Research/armada/pkg/armadaevents"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/app"
	"github.com/G-Research/armada/internal/common/ingest"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Scheduler database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *Configuration) {
	metrics := metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "armada_scheduler_ingester_")

	log.Infof("Opening connection pool to postgres")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	schedulerDb := NewSchedulerDb(db, metrics, 100*time.Millisecond, 60*time.Second)

	// Discard submit job messages not intended for this scheduler.
	submitJobFilter := func(event *armadaevents.EventSequence_Event) bool {
		// Discard if a SubmitJob event that doesn't target this scheduler.
		if e := event.GetSubmitJob(); e != nil && e.Scheduler != "pulsar" {
			return false
		}
		return true
	}

	converter := NewInstructionConverter(metrics, submitJobFilter)

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
