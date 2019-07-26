package main

import (
	"github.com/G-Research/k8s-batch/internal/armada"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/G-Research/k8s-batch/internal/common"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	common.ConfigureLogging()
	var config configuration.ArmadaConfig
	common.LoadConfig(&config, "./config/armada")

	log.Info("Starting...")

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	s, wg := armada.Serve(&config)
	go func() {
		<-stopSignal
		s.Stop()
	}()
	wg.Wait()
}
