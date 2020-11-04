package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/serve"
	"github.com/G-Research/armada/internal/lookout"
	"github.com/G-Research/armada/internal/lookout/configuration"
	lookoutApi "github.com/G-Research/armada/pkg/api/lookout"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutConfiguration
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookout", userSpecifiedConfig)

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	mux, shutdownGateway := grpc.CreateGatewayHandler(
		config.GrpcPort,
		"/api/",
		lookoutApi.SwaggerJsonTemplate(),
		lookoutApi.RegisterLookoutHandler)

	// server static UI files
	mux.Handle("/", http.FileServer(serve.CreateDirWithIndexFallback("./internal/lookout/ui/build")))

	shutdownServer := common.ServeHttp(config.HttpPort, mux)

	shutdown, wg := lookout.StartUp(config)
	go func() {
		<-shutdownChannel
		shutdown()
		shutdownGateway()
		shutdownServer()
	}()
	wg.Wait()
}
