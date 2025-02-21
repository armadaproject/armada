package main

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookoutingester"
	"github.com/armadaproject/armada/internal/lookoutingester/benchmark"
	"github.com/armadaproject/armada/internal/lookoutingester/configuration"
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
	log.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutIngesterConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)

	common.LoadConfig(&config, "./config/lookoutingester", userSpecifiedConfigs)

	runBenchmarks := viper.GetBool(Benchmark)
	if runBenchmarks {
		log.Info("Running Lookout Ingester benchmarks")
		benchmark.RunBenchmark(config)
		return
	}

	lookoutingester.Run(&config)
}
