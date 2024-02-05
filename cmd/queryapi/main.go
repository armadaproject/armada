package main

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/queryapi"
)

const (
	CustomConfigLocation = "config"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config queryapi.Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/queryapi", userSpecifiedConfigs)
	queryapi.Run(config)
}
