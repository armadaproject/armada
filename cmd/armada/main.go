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
	pflag.StringSlice(CustomConfigLocation, []string{}, "Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.ArmadaConfig
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfigs)

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
