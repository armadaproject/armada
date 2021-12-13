package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/armada"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	gateway "github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/health"
	"github.com/G-Research/armada/pkg/api"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.StringSlice(CustomConfigLocation, []string{}, "Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	// TODO Load relevant config in one place: don't use viper here and in the config package
	// (currently in common).
	var config configuration.ArmadaConfig
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfigs)

	log.Info("Starting...")

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	// TODO This starts a separate HTTP server. Is that intended? Should we have a single mux for everything?
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	mux := http.NewServeMux()

	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)

	// Register /health API endpoint
	health.SetupHttpMux(mux, healthChecks)

	// register gRPC API handlers in mux
	shutdownGateway := gateway.CreateGatewayHandler(
		config.GrpcPort, mux, "/",
		config.CorsAllowedOrigins,
		api.SwaggerJsonTemplate(),
		api.RegisterSubmitHandler,
		api.RegisterEventHandler,
	)
	defer shutdownGateway()

	// start HTTP server
	shutdownHttpServer := common.ServeHttp(config.HttpPort, mux)
	defer shutdownHttpServer()

	shutdown, wg := armada.Serve(&config, healthChecks)
	go func() {
		<-stopSignal

		// TODO Can't be sure this will run, since there are calls to panic/os.Exit in sub-calls
		shutdown()
	}()

	// TODO Why do we have this system?
	// Instead, perform checks before starting services that depend on these checks being completed.
	startupCompleteCheck.MarkComplete()
	wg.Wait()
}
