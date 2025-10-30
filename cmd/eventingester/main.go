package main

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/tracing"
	"github.com/armadaproject/armada/internal/eventingester"
	"github.com/armadaproject/armada/internal/eventingester/configuration"
)

const (
	CustomConfigLocation string = "config"
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
	logging.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()

	// Initialize tracing
	cleanup, err := tracing.InitTracing("event-ingester")
	if err != nil {
		fmt.Printf("Failed to initialize tracing: %v\n", err)
		os.Exit(-1)
	}
	defer cleanup()

	var config configuration.EventIngesterConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/eventingester", userSpecifiedConfigs)
	eventingester.Run(&config)
}
