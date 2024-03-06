package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/serve"
	"github.com/armadaproject/armada/internal/executor"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/metrics"
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

	var config configuration.ExecutorConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/executor", userSpecifiedConfigs)

	// Expose profiling endpoints if enabled.
	if config.PprofPort != nil {
		pprofServer := profiling.SetupPprofHttpServer(*config.PprofPort)
		go func() {
			ctx := armadacontext.Background()
			if err := serve.ListenAndServe(ctx, pprofServer); err != nil {
				logging.WithStacktrace(ctx, err).Error("pprof server failure")
			}
		}()
	}

	mux := http.NewServeMux()
	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)
	health.SetupHttpMux(mux, healthChecks)

	shutdownHttpServer := common.ServeHttp(config.HttpPort, mux)
	defer shutdownHttpServer()

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetricsFor(
		config.Metric.Port,
		prometheus.Gatherers{metrics.GetMetricsGatherer()},
	)
	defer shutdownMetricServer()

	shutdown, wg := executor.StartUp(armadacontext.Background(), logrus.NewEntry(logrus.New()), config)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	startupCompleteCheck.MarkComplete()
	wg.Wait()
}
