package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/fake"
	"github.com/G-Research/armada/internal/executor/fake/context"
)

const CustomConfigLocation string = "config"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.ExecutorConfiguration
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	v := common.LoadConfig(&config, "./config/executor", userSpecifiedConfig)

	var nodes []*context.NodeSpec
	e := common.UnmarshalKey(v, "nodes", &nodes)
	if e != nil {
		panic(e)
	}

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.Metric.Port)
	defer shutdownMetricServer()

	shutdown, wg := fake.StartUp(config, nodes)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	wg.Wait()
}
