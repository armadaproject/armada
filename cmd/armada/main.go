package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/armada"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/pkg/api"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.ArmadaConfig
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfig)

	log.Info("Starting...")

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	shutdownGateway := grpc.ServeGateway(
		config.HttpPort,
		config.GrpcPort,
		config.CorsAllowedOrigins,
		api.SwaggerJsonTemplate(),
		api.RegisterSubmitHandler,
		api.RegisterEventHandler,
	)
	defer shutdownGateway()

	shutdown, wg := armada.Serve(&config)
	go func() {
		<-stopSignal
		shutdown()
	}()
	wg.Wait()
}
