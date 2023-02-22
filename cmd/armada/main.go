package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"golang.org/x/sync/errgroup"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	exp, err := newExporter("http://localhost:14268/api/traces")
	if err != nil {
		log.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exp)),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()
	otel.SetTracerProvider(tp)

	log.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
		log.WarnLevel,
	)))
	log.SetFormatter(&log.JSONFormatter{})

	// TODO Load relevant config in one place: don't use viper here and in the config package
	// (currently in common).
	var config configuration.ArmadaConfig
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfigs)

	log.Info("Starting...")

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

func newExporter(url string) (trace.SpanExporter, error) {
	return jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
}

// newResource returns a resource describing this application.
func newResource() *resource.Resource {
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("server"),
			semconv.ServiceVersionKey.String("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	return r
}
