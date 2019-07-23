package main

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/startup"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	configuration := startup.LoadConfiguration()

	wg, shutdownChannel := startup.StartUp(configuration)

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
