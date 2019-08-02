package common

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
)

func BindCommandlineArguments() {
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Error()
		os.Exit(-1)
	}
}

func LoadConfig(config interface{}, defaultPath string, overrideConfig string) {
	viper.SetConfigName("config")
	viper.AddConfigPath(defaultPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	if overrideConfig != "" {
		viper.SetConfigFile(overrideConfig)

		err := viper.MergeInConfig()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
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
