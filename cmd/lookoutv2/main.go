package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookoutv2"
	"github.com/armadaproject/armada/internal/lookoutv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutv2/pruner"
	"github.com/armadaproject/armada/internal/lookoutv2/schema"
)

const (
	CustomConfigLocation string = "config"
	MigrateDatabase             = "migrateDatabase"
	PruneDatabase               = "pruneDatabase"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Bool(PruneDatabase, false, "Prune database of old jobs instead of running server")
	pflag.Parse()
}

func makeContext() (context.Context, func()) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

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

func migrate(ctx context.Context, config configuration.LookoutV2Configuration) {
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(err)
	}

	migrations, err := schema.LookoutMigrations()
	if err != nil {
		panic(err)
	}

	err = database.UpdateDatabase(ctx, db, migrations)
	if err != nil {
		panic(err)
	}
}

func prune(ctx context.Context, config configuration.LookoutV2Configuration) {
	db, err := database.OpenPgxConn(config.Postgres)
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
	log.Infof("expireAfter: %v, batchSize: %v, timeout: %v",
		config.PrunerConfig.ExpireAfter, config.PrunerConfig.BatchSize, config.PrunerConfig.Timeout)

	ctxTimeout, cancel := context.WithTimeout(ctx, config.PrunerConfig.Timeout)
	defer cancel()
	err = pruner.PruneDb(ctxTimeout, db, config.PrunerConfig.ExpireAfter, config.PrunerConfig.BatchSize, clock.RealClock{})
	if err != nil {
		panic(err)
	}
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutV2Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookoutv2", userSpecifiedConfigs)

	log.SetLevel(log.DebugLevel)

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

	err := lookoutv2.Serve(config)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
