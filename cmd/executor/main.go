package main

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	common.ConfigureLogging()
	var config configuration.ExecutorConfiguration
	common.LoadConfig(&config, "./config/executor")

	wg, shutdownChannel := executor.StartUp(config)

	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)
	defer handleGracefulShutdownDuringPanic(wg, shutdownChannel)

	wg.Wait()
}

func handleGracefulShutdownDuringPanic(wg *sync.WaitGroup, shutdownChannel chan os.Signal) {
	if err := recover(); err != nil {
		sig := domain.PanicSignal{}
		shutdownChannel <- sig
		wg.Wait()
		panic(err)
	}
}
