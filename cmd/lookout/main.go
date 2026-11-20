package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/observability"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/lookout"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/gen/restapi"
	"github.com/armadaproject/armada/internal/lookout/pruner"
	lookoutschema "github.com/armadaproject/armada/internal/lookout/schema"
	armada_config "github.com/armadaproject/armada/internal/server/configuration"
)

const (
	CustomConfigLocation string = "config"
	MigrateDatabase             = "migrateDatabase"
	PruneDatabase               = "pruneDatabase"
	PartitionJobTable           = "partitionJobTable"
	Preflight                   = "preflight"
	PrintSql                    = "printSql"
	Execute                     = "execute"
	Yes                         = "yes"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Bool(PruneDatabase, false, "Prune database of old jobs instead of running server")
	pflag.Bool(PartitionJobTable, false, "Partition the job table instead of running server; reports on it with --preflight unless --printSql or --execute is given")
	pflag.Bool(Preflight, false, "With --partitionJobTable, report on the job table and the database without changing them (the default)")
	pflag.Bool(PrintSql, false, "With --partitionJobTable, print the SQL that --execute runs to stdout, and logs to stderr")
	pflag.Bool(Execute, false, "With --partitionJobTable, partition the job table")
	pflag.Bool(Yes, false, "With --partitionJobTable --execute, confirm without prompting")
	pflag.Parse()
}

func makeContext() (*armadacontext.Context, func()) {
	ctx := armadacontext.Background()
	ctx, cancel := armadacontext.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(c)
		cancel()
	}
}

func migrate(ctx *armadacontext.Context, config configuration.LookoutConfig) {
	db, err := database.OpenPgxConn(config.Postgres)
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	if err := database.PrepareSchema(ctx, db, config.Migration); err != nil {
		panic(err)
	}

	migrations, err := lookoutschema.LookoutMigrations()
	if err != nil {
		panic(err)
	}

	err = lookoutschema.UpdateDatabase(ctx, db, migrations)
	if err != nil {
		panic(err)
	}

	shape, err := lookoutschema.PartitionJobTableIfEmpty(ctx, db)
	if err != nil {
		panic(err)
	}
	if shape == lookoutschema.JobTableShapeUnpartitioned {
		log.Warn(lookoutschema.LegacySchemaWarning)
	} else {
		log.Infof("Job table shape: %s", shape)
	}
}

func prune(ctx *armadacontext.Context, config configuration.LookoutConfig) {
	var dbConfig armada_config.PostgresConfig
	if config.PrunerConfig.Postgres.Connection != nil {
		dbConfig = config.PrunerConfig.Postgres
	} else {
		dbConfig = config.Postgres
	}

	db, err := database.OpenPgxConn(dbConfig)
	if err != nil {
		panic(err)
	}

	if config.PrunerConfig.Timeout <= 0 {
		panic("timeout must be greater than 0")
	}
	if config.PrunerConfig.ExpireAfter <= 0 {
		panic("expireAfter must be greater than 0")
	}
	if config.PrunerConfig.BatchSize <= 0 {
		panic("batchSize must be greater than 0")
	}
	zombieRepairThreshold := 15 * time.Minute
	if config.PrunerConfig.ZombieRepairThreshold != nil {
		zombieRepairThreshold = *config.PrunerConfig.ZombieRepairThreshold
	}
	log.Infof("expireAfter: %v, batchSize: %v, timeout: %v, zombieRepairThreshold: %v",
		config.PrunerConfig.ExpireAfter, config.PrunerConfig.BatchSize, config.PrunerConfig.Timeout, zombieRepairThreshold)

	ctxTimeout, cancel := armadacontext.WithTimeout(ctx, config.PrunerConfig.Timeout)
	defer cancel()
	err = pruner.PruneDb(
		ctxTimeout,
		db,
		config.PrunerConfig.ExpireAfter,
		config.PrunerConfig.DeduplicationExpireAfter,
		zombieRepairThreshold,
		config.PrunerConfig.BatchSize,
		clock.RealClock{},
	)
	if err != nil {
		panic(err)
	}

	if config.PrunerConfig.PushgatewayUrl != "" {
		jobName := config.PrunerConfig.PushgatewayJobName
		if jobName == "" {
			jobName = "lookout-pruner"
		}
		pushCtx, pushCancel := armadacontext.WithTimeout(ctx, 30*time.Second)
		defer pushCancel()
		if err := pruner.PushMetrics(pushCtx, config.PrunerConfig.PushgatewayUrl, jobName); err != nil {
			log.WithError(err).Warn("failed to push pruner metrics to Pushgateway")
		}
	}
}

func main() {
	log.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()
	if viper.GetBool(PrintSql) {
		log.ReplaceStdLogger(log.FromZerolog(zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, NoColor: true}).With().Timestamp().Logger()))
	}

	var config configuration.LookoutConfig

	// Initialise slices to prevent JSON marshalling to 'null' for non-existent slices
	config.CorsAllowedOrigins = make([]string, 0)
	config.UIConfig.CommandSpecs = make([]configuration.CommandSpec, 0)
	config.JobLinks = make([]configuration.JobLinkConfig, 0)
	config.PinnedTimeZoneIdentifiers = make([]string, 0)

	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookout", userSpecifiedConfigs)

	// Initialize OpenTelemetry
	if err := observability.InitOTel(config.Observability); err != nil {
		log.Fatalf("Failed to initialize OTel: %v", err)
	}
	defer func() {
		if err := observability.ShutdownWithDefaultTimeout(); err != nil {
			log.Warnf("Failed to shutdown OTel: %v", err)
		}
	}()

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	ctx, cleanup := makeContext()
	defer cleanup()

	if viper.GetBool(MigrateDatabase) {
		log.Info("Migrating database")
		migrate(ctx, config)
		return
	}

	if viper.GetBool(PruneDatabase) {
		log.Info("Pruning database")
		prune(ctx, config)
		return
	}

	if viper.GetBool(PartitionJobTable) || viper.GetBool(Preflight) || viper.GetBool(PrintSql) || viper.GetBool(Execute) || viper.GetBool(Yes) {
		if err := partitionJobTable(ctx, config); err != nil {
			log.Error(err.Error())
			os.Exit(1)
		}
		return
	}

	restapi.UIConfig = config.UIConfig

	if err := lookout.Serve(config); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
