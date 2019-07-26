package main

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	common.ConfigureLogging()
	var config configuration.ExecutorConfiguration
	common.LoadConfig(&config, "./config/executor")

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdown, wg := executor.StartUp(config)
	go func() {
		<-shutdownChannel
		shutdown()
	}()
	wg.Wait()
}
