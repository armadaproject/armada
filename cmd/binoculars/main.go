package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/binoculars"
	"github.com/armadaproject/armada/internal/binoculars/configuration"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	gateway "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/serve"
	api "github.com/armadaproject/armada/pkg/api/binoculars"
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

	var config configuration.BinocularsConfig
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/binoculars", userSpecifiedConfigs)

	log.Info("Starting...")

	// Expose profiling endpoints if enabled.
	pprofServer := profiling.SetupPprofHttpServer(config.PprofPort)
	go func() {
		ctx := armadacontext.Background()
		if err := serve.ListenAndServe(ctx, pprofServer); err != nil {
			logging.WithStacktrace(ctx, err).Error("pprof server failure")
		}
	}()

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	mux := http.NewServeMux()

	startupComplete := health.NewStartupCompleteChecker()
	health.SetupHttpMux(mux, startupComplete)

	shutdownGateway := serveHttp(
		mux,
		config.HttpPort,
		config.GrpcPort,
		config.CorsAllowedOrigins,
		api.SwaggerJsonTemplate(),
		api.RegisterBinocularsHandler,
	)
	defer shutdownGateway()

	shutdown, wg := binoculars.StartUp(&config)
	go func() {
		<-stopSignal
		shutdown()
	}()
	startupComplete.MarkComplete()
	wg.Wait()
}

func serveHttp(
	mux *http.ServeMux,
	port uint16,
	grpcPort uint16,
	corsAllowedOrigins []string,
	spec string,
	handlers ...func(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error,
) (shutdown func()) {
	shutdownGateway := gateway.CreateGatewayHandler(grpcPort, mux, "/", false, false, corsAllowedOrigins, spec, handlers...)
	cancel := common.ServeHttp(port, mux)

	return func() {
		shutdownGateway()
		cancel()
	}
}
