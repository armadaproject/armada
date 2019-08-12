package main

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
)

const CustomConfigLocation string = "config"
const ApiCredentialsLocation string = "apiCredentialsPath"
const InCluster string = "inCluster"
const KubeConfig string = "kubeConfigPath"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.String(ApiCredentialsLocation, "", "Fully qualified path to api credentials file")
	pflag.Bool(InCluster, false, "When set executor will run using in cluster client connection details")
	pflag.String(KubeConfig, "", "Fully qualified path to custom kube config file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()
	config := loadConfig()

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdown, wg := executor.StartUp(config)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	wg.Wait()
}

func loadConfig() configuration.ExecutorConfiguration {
	var config configuration.ExecutorConfiguration
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/executor", userSpecifiedConfig)
	loadCredentials(&config)

	log.Info("Username " + config.Authentication.Username)
	log.Info("Password " + config.Authentication.Password)

	inClusterDeployment := viper.GetBool(InCluster)
	customKubeConfigLocation := viper.GetString(KubeConfig)

	config.Kubernetes = configuration.KubernetesConfiguration{
		InClusterDeployment:      inClusterDeployment,
		KubernetesConfigLocation: customKubeConfigLocation,
	}
	return config
}

func loadCredentials(config *configuration.ExecutorConfiguration) {
	credentialsPath := viper.GetString(ApiCredentialsLocation)
	if credentialsPath != "" {
		viper.SetConfigFile(credentialsPath)

		err := viper.ReadInConfig()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
		username := viper.GetString(common.UsernameField)
		password := viper.GetString(common.PasswordField)

		config.Authentication = configuration.AuthenticationConfiguration{
			EnableAuthentication: true,
			Username:             username,
			Password:             password,
		}
	}
}
