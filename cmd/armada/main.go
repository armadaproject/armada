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
)

const CustomConfigLocation string = "config"
const ValidUsersLocation string = "usersPath"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.String(ValidUsersLocation, "", "Fully qualified path to user credentials file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.ArmadaConfig
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/armada", userSpecifiedConfig)
	loadUsersCredentialFile(&config)

	log.Info("Starting...")
	log.Infof("Config %+v", config)

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(":9000")
	defer shutdownMetricServer()

	s, wg := armada.Serve(&config)
	go func() {
		<-stopSignal
		s.GracefulStop()
	}()
	wg.Wait()
}

func loadUsersCredentialFile(config *configuration.ArmadaConfig) {
	credentialsPath := viper.GetString(ValidUsersLocation)

	if credentialsPath != "" {
		log.Info("Loading credentials from " + credentialsPath)
		viper.SetConfigFile(credentialsPath)

		err := viper.ReadInConfig()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
		users := viper.GetStringMapString("users")

		config.BasicAuth = configuration.BasicAuthenticationConfig{
			EnableAuthentication: true,
			Users:                users,
		}
	}
}
