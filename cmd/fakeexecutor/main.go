package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/fake"
	"github.com/armadaproject/armada/internal/executor/fake/context"
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

	var config configuration.ExecutorConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	v := common.LoadConfig(&config, "./config/executor", userSpecifiedConfigs)

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
