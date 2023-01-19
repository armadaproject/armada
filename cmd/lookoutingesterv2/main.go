package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/lookoutingesterv2"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/benchmark"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
)

const (
	CustomConfigLocation = "config"
	Benchmark            = "bench"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Bool(Benchmark, false, "Whether to run Lookout Ingester benchmarks instead of the application")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutIngesterV2Configuration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/lookoutingesterv2", userSpecifiedConfigs)

	runBenchmarks := viper.GetBool(Benchmark)
	if runBenchmarks {
		log.Info("Running Lookout Ingester benchmarks")
		benchmark.RunBenchmark(config)
		return
	}

	lookoutingesterv2.Run(&config)
}
