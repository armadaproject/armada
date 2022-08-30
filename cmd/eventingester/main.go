package main

import (
	ctx "context"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/eventapi/configuration"
	"github.com/G-Research/armada/internal/eventapi/eventdb/schema/statik"
	"github.com/G-Research/armada/internal/eventapi/ingestion"
	"github.com/G-Research/armada/internal/lookout/postgres"

	"github.com/G-Research/armada/internal/common"
)

const (
	CustomConfigLocation string = "config"
	MigrateDatabase      string = "migrateDatabase"
)

func init() {
	pflag.StringSlice(CustomConfigLocation, []string{}, "Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.EventIngesterConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/eventingester", userSpecifiedConfigs)
	if viper.GetBool(MigrateDatabase) {
		err := migrateDatabase(config)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		ingestion.Run(&config)
	}
}

func migrateDatabase(config configuration.EventIngesterConfiguration) error {
	log.Infof("Opening connection pool to postgres")
	db, err := postgres.OpenPgxPool(config.Postgres)
	defer db.Close()
	if err != nil {
		return err
	}
	migrations, err := database.GetMigrations(statik.EventapiSql)
	if err != nil {
		return err
	}
	return database.UpdateDatabase(ctx.Background(), db, migrations)
}
