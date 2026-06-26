package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/health"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/observability"
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
	log.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()

	var config configuration.ExecutorConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/executor", userSpecifiedConfigs)

	if err := observability.InitOTel(config.Observability); err != nil {
		log.Warnf("Failed to initialize OTel: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := observability.ShutdownOTel(ctx); err != nil {
			log.Warnf("Failed to shutdown OTel: %v", err)
		}
	}()

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
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
		prometheus.Gatherers{prometheus.DefaultGatherer},
	)
	defer shutdownMetricServer()

	shutdown, wg := executor.StartUp(armadacontext.Background(), config)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	startupCompleteCheck.MarkComplete()
	wg.Wait()
}
