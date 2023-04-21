package lookoutingesterv2

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/app"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/ingest"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/instructions"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/metrics"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/model"
)

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Lookout database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.LookoutIngesterV2Configuration) {
	log.Infof("Opening connection pool to postgres")
	m := metrics.Get()
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	lookoutDb := lookoutdb.NewLookoutDb(db, m, config.MaxAttempts, config.MaxBackoff)

	compressor, err := compress.NewZlibCompressor(config.MinJobSpecCompressionSize)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating compressor"))
	}

	converter := instructions.NewInstructionConverter(m, config.UserAnnotationPrefix, compressor)

	ingester := ingest.NewIngestionPipeline[*model.InstructionSet](
		config.Pulsar,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		pulsar.KeyShared,
		converter,
		lookoutDb,
		config.Metrics,
		m)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
