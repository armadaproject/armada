package scheduleringester

import (
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	controlplaneevents_ingest_utils "github.com/armadaproject/armada/internal/common/ingest/controlplaneevents"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	log "github.com/armadaproject/armada/internal/common/loggin
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
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

	jobSetEventsConverter, err := NewJobSetEventsInstructionConverter(svcMetrics)
	if err != nil {
		return err
	}

	// Expose profiling endpoints if enabled.
	err = profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	// Start metric server
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	jobSetEventsIngester := ingest.NewIngestionPipeline[*DbOperationsWithMessageIds, *armadaevents.EventSequence](
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
		jobSetEventsConverter,
		schedulerDb,
		svcMetrics,
	)

	controlPlaneEventsConverter, err := NewControlPlaneEventsInstructionConverter(svcMetrics)
	if err != nil {
		return err
	}

	controlPlaneEventsIngester := ingest.NewIngestionPipeline[*DbOperationsWithMessageIds, *controlplaneevents.Event](
		config.Pulsar,
		config.Pulsar.ControlPlaneEventsTopic,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		pulsar.Failover,
		controlplaneevents_ingest_utils.EventCounter,
		controlplaneevents_ingest_utils.MessageUnmarshaller,
		controlplaneevents_ingest_utils.BatchMerger,
		controlplaneevents_ingest_utils.BatchMetricPublisher,
		controlPlaneEventsConverter,
		schedulerDb,
		svcMetrics,
	)

	wg := sync.WaitGroup{}

	// Starting the jobSet events ingestion pipeline
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := jobSetEventsIngester.Run(app.CreateContextWithShutdown()); err != nil {
			log.Errorf("error running jobSet event ingestion pipeline: %s", err)
		}
	}()

	// Starting the Control Plane events ingestion pipeline
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := controlPlaneEventsIngester.Run(app.CreateContextWithShutdown()); err != nil {
			log.Errorf("error running control plane event ingestion pipeline: %s", err)
		}
	}()

	wg.Wait()

	return nil
}
