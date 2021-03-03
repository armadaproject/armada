package main

import (
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
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
	common.LoadConfig(&config, "./config", userSpecifiedConfig)

	db, err := postgres.Open(config.Postgres)
	if err != nil {
		panic(err)
	}

	// TODO:
	// * CircleCI building & deployment
	// * Remove migration from normal lookout
	// * K8s stuff?
	err = schema.UpdateDatabase(db)
	if err != nil {
		panic(err)
	}
}
