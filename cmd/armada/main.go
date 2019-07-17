package main

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada"
	"github.com/G-Research/k8s-batch/internal/armada/configuration"
	"github.com/spf13/viper"
	"os"
)

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath("./config/executor")
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

	_, wg := armada.Serve(&config)
	wg.Wait()
}
