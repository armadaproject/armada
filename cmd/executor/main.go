package main

import (
	"github.com/armadaproject/armada/internal/common/logging"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/executor"
	"github.com/armadaproject/armada/internal/executor/configuration"
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
	common.ConfigureLogging()
	common.BindCommandlineArguments()
	ctx := armadacontext.New(armadacontext.Background(), logging.NewLogger())

	var config configuration.ExecutorConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/executor", userSpecifiedConfigs)

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, ctx, nil)
	if err != nil {
		ctx.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	mux := http.NewServeMux()
	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)
	health.SetupHttpMux(mux, healthChecks)

	shutdownHttpServer := common.ServeHttp(ctx, config.HttpPort, mux)
	defer shutdownHttpServer()

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetricsFor(
		ctx,
		config.Metric.Port,
		prometheus.Gatherers{prometheus.DefaultGatherer},
	)
	defer shutdownMetricServer()

	shutdown, wg := executor.StartUp(ctx, config)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	startupCompleteCheck.MarkComplete()
	wg.Wait()
}
