package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor"
	"github.com/G-Research/armada/internal/executor/configuration"
)

const CustomConfigLocation string = "config"
const InCluster string = "inCluster"
const KubeConfig string = "kubeConfigPath"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
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

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

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

	inClusterDeployment := viper.GetBool(InCluster)
	customKubeConfigLocation := viper.GetString(KubeConfig)

	config.Kubernetes = configuration.KubernetesConfiguration{
		InClusterDeployment:      inClusterDeployment,
		KubernetesConfigLocation: customKubeConfigLocation,
	}
	return config
}
