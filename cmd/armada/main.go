package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/armada"
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common"
	gateway "github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
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

	// TODO Load relevant config in one place: don't use viper here and in the config package
	// (currently in common).
	var config configuration.ArmadaConfig
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfigs)

	log.Info("Starting...")

	// net/http/pprof automatically binds to http.DefaultServeMux.
	// Create a new instance to ensure only profiling is exposed on this mux.
	// The endpoints are only exposed if config.ProfilingPort is not nil.
	pprofMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	if config.ProfilingPort != nil {
		go func() {
			server := &http.Server{
				Addr:    fmt.Sprintf("localhost:%d", *config.ProfilingPort),
				Handler: pprofMux,
			}
			log.Error(server.ListenAndServe())
		}()
	}

	// Run services within an errgroup to propagate errors between services.
	g, ctx := errgroup.WithContext(context.Background())

	// Cancel the errgroup context on SIGINT and SIGTERM,
	// which shuts everything down gracefully.
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case sig := <-stopSignal:
			// Returning an error cancels the errgroup.
			return fmt.Errorf("received signal %v", sig)
		}
	})

	// TODO This starts a separate HTTP server. Is that intended? Should we have a single mux for everything?
	// TODO: Run in errgroup
	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	// Register /health API endpoint
	mux := http.NewServeMux()
	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)
	health.SetupHttpMux(mux, healthChecks)

	pprof.Handler()

	// register gRPC API handlers in mux
	// TODO: Run in errgroup
	shutdownGateway := gateway.CreateGatewayHandler(
		config.GrpcPort, mux, "/",
		config.CorsAllowedOrigins,
		api.SwaggerJsonTemplate(),
		api.RegisterSubmitHandler,
		api.RegisterEventHandler,
	)
	defer shutdownGateway()

	// start HTTP server
	// TODO: Run in errgroup
	shutdownHttpServer := common.ServeHttp(config.HttpPort, mux)
	defer shutdownHttpServer()

	// Start Armada server
	g.Go(func() error {
		return armada.Serve(ctx, &config, healthChecks)
	})

	// Assume the server is ready if there are no errors within 10 seconds.
	go func() {
		time.Sleep(10 * time.Second)
		startupCompleteCheck.MarkComplete()
	}()

	if err := g.Wait(); err != nil {
		logging.WithStacktrace(log.NewEntry(log.StandardLogger()), err).Error("Armada server shut down")
	}
}
