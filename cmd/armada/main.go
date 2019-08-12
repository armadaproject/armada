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

	log.Infof("Users  %d", len(config.Authentication.Users))
	for _, user := range config.Authentication.Users {
		log.Info(user)
	}

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

func loadUsersCredentialFile(config *configuration.ArmadaConfig) {
	credentialsPath := viper.GetString(ValidUsersLocation)
	if credentialsPath != "" {
		viper.SetConfigFile(credentialsPath)

		err := viper.ReadInConfig()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
		users := viper.GetStringMapString("users")

		config.Authentication = configuration.AuthenticationConfig{
			EnableAuthentication: true,
			Users:                users,
		}
	}
}
