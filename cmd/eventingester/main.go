package main

import (
	"context"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/observability"
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

	var config configuration.EventIngesterConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/eventingester", userSpecifiedConfigs)

	// Initialize OpenTelemetry
	if err := observability.InitOTel(config.Observability); err != nil {
		logging.Warnf("Failed to initialize OTel: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := observability.ShutdownOTel(ctx); err != nil {
			logging.Warnf("Failed to shutdown OTel: %v", err)
		}
	}()

	eventingester.Run(&config)
}
