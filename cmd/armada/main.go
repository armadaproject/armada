package main

import (
	"github.com/G-Research/k8s-batch/internal/armada"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
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

	s, wg := armada.Serve(&config)
	go func() {
		<-stopSignal
		s.GracefulStop()
	}()
	wg.Wait()
}
