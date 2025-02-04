package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/scheduleringester"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Parse()
}

func main() {
	logging.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()

	var config scheduleringester.Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/scheduleringester", userSpecifiedConfigs)

	if err := scheduleringester.Run(config); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
