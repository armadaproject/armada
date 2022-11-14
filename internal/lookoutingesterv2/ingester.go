package lookoutingesterv2

import (
	"github.com/G-Research/armada/internal/common/app"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/lookoutingesterv2/configuration"
	"github.com/G-Research/armada/internal/lookoutingesterv2/instructions"
	"github.com/G-Research/armada/internal/lookoutingesterv2/lookoutdb"
	"github.com/G-Research/armada/internal/lookoutingesterv2/metrics"
	"github.com/G-Research/armada/internal/lookoutingesterv2/model"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
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
	lookoutDb := lookoutdb.NewLookoutDb(db, m, 10, 60)

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
		converter,
		lookoutDb,
		config.Metrics,
		m)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}
