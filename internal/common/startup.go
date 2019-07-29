package common

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

func LoadConfig(config interface{}, path string) {
	viper.SetConfigName("config")
	viper.AddConfigPath(path)
	if err := viper.ReadInConfig(); err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	err := viper.Unmarshal(config)
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
}

func ConfigureLogging() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true})
	log.SetOutput(os.Stdout)
}
