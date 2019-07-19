package main

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath("./config/armada")
	var config configuration.ArmadaConfig

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	err := viper.Unmarshal(&config)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)

	s, wg := armada.Serve(&config)
	go func() {
		<-stopSignal
		s.Stop()
	}()
	wg.Wait()
}
