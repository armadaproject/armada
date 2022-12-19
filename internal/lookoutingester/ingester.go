package lookoutingester

import (
	"os"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	armadaconfig "github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/app"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/G-Research/armada/internal/lookoutingester/instructions"
	"github.com/G-Research/armada/internal/lookoutingester/lookoutdb"
	"github.com/G-Research/armada/internal/lookoutingester/metrics"
)

const (
	MigrateDatabase string = "migrateDatabase"
)

func init() {
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running lookout-ingester")
}

// Run will create a pipeline that will take Armada event messages from Pulsar and update the
// Lookout database accordingly.  This pipeline will run until a SIGTERM is received
func Run(config *configuration.LookoutIngesterConfiguration) {
	if viper.GetBool(MigrateDatabase) {
		log.Infof("Got migration flag, migrating lookout DB and quitting.")
		doMigration(config.Postgres)
		log.Infof("Migration done.")
		os.Exit(0)
	}

	log.Infof("Opening connection pool to postgres")
	metrics := metrics.Get()
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Error opening connection to postgres"))
	}
	lookoutDb := lookoutdb.NewLookoutDb(db, metrics)

	compressor, err := compress.NewZlibCompressor(config.MinJobSpecCompressionSize)
	if err != nil {
		panic(errors.WithMessage(err, "Error creating compressor"))
	}

	converter := instructions.NewInstructionConverter(metrics, config.UserAnnotationPrefix, compressor)

	ingester := ingest.NewIngestionPipeline(
		config.Pulsar,
		config.SubscriptionName,
		config.BatchSize,
		config.BatchDuration,
		converter,
		lookoutDb,
		config.Metrics,
		metrics)

	err = ingester.Run(app.CreateContextWithShutdown())
	if err != nil {
		panic(errors.WithMessage(err, "Error running ingestion pipeline"))
	}
}

func doMigration(pgConfig armadaconfig.PostgresConfig) {
	db, err := postgres.Open(pgConfig)
	if err != nil {
		panic(err)
	}

	err = schema.UpdateDatabase(db)
	if err != nil {
		panic(err)
	}
}
