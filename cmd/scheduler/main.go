package main

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/scheduler"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

const (
	CustomConfigLocation string = "config"
	MigrateDatabase      string = "migrateDatabase"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running scheduler")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config scheduler.Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/scheduler", userSpecifiedConfigs)

	if viper.GetBool(MigrateDatabase) {
		migrateDatabase(&config)
	} else {
		if err := scheduler.Run(config); err != nil {
			log.WithError(err).Errorf("Scheduler failed")
			os.Exit(1)
		}
	}
}

func migrateDatabase(config *scheduler.Configuration) {
	start := time.Now()
	log.Info("Beginning scheduler database migration")
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		panic(errors.WithMessage(err, "Failed to connect to database"))
	}
	err = schedulerdb.Migrate(context.Background(), db)
	if err != nil {
		panic(errors.WithMessage(err, "Failed to migrate scheduler database"))
	}
	taken := time.Now().Sub(start)
	log.Infof("Scheduler database migrated in %dms", taken.Milliseconds())
	os.Exit(0)
}
