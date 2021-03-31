package main

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to migration configuration file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutMigrationConfiguration
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookout-migration", userSpecifiedConfig)

	db, err := postgres.Open(config.Postgres)
	if err != nil {
		panic(err)
	}

	err = schema.UpdateDatabase(db)
	if err != nil {
		panic(err)
	}
}
