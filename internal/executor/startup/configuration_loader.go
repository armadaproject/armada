package startup

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"github.com/spf13/viper"
	"os"
)

func LoadConfiguration() configuration.Configuration {
	viper.SetConfigName("config")
	viper.AddConfigPath("./config/executor")
	var config configuration.Configuration

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	err := viper.Unmarshal(&config)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	return config
}
