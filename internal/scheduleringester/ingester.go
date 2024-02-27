package scheduleringester

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/serve"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/schedulers"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the schedulerDb.
// This pipeline will run until a SIGTERM is received.
func Run(config Configuration) error {
	svcMetrics := metrics.NewMetrics(metrics.ArmadaEventIngesterMetricsPrefix + "armada_scheduler_ingester_")

	log.Infof("opening connection pool to postgres")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	schedulerDb := NewSchedulerDb(db, svcMetrics, 100*time.Millisecond, 60*time.Second, 5*time.Second)

	converter, err := NewInstructionConverter(svcMetrics, config.PriorityClasses)
	if err != nil {
		return err
	}

	// Expose profiling endpoints if enabled.
	if config.PprofPort != nil {
		pprofServer := profiling.SetupPprofHttpServer(*config.PprofPort)
		go func() {
			ctx := armadacontext.Background()
			if err := serve.ListenAndServe(ctx, pprofServer); err != nil {
				logging.WithStacktrace(ctx, err).Error("pprof server failure")
			}
		}()
	}

	ingester := ingest.NewFilteredMsgIngestionPipeline[*DbOperationsWithMessageIds](
		config.Pulsar,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		pulsar.Failover,
		schedulers.ForPulsarScheduler,
		converter,
		schedulerDb,
		config.MetricsPort,
		svcMetrics,
	)
	if err := ingester.Run(app.CreateContextWithShutdown()); err != nil {
		return errors.WithMessage(err, "error running ingestion pipeline")
	}
	return nil
}
