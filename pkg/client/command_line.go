package client

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
}

func LoadCommandlineArgsFromConfigFile(cfgFile string) {
	exePath, err := os.Executable()
	if err != nil {

		log.Errorf("Can't find executable path because %s\n", err)
		os.Exit(1)
	} else {
		exeDir := filepath.Dir(exePath)
		viper.SetConfigFile(exeDir + "/armadactl-defaults.yaml")
		err := viper.ReadInConfig()
		if err != nil {
			switch err.(type) {
			case viper.ConfigFileNotFoundError:
			case *os.PathError:
				// No default config is fine
			default:
				log.Errorf("Can't read config file %s because %s\n", viper.ConfigFileUsed(), err)
				os.Exit(1)
			}
		}
	}

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		viper.AddConfigPath(home)
		viper.SetConfigName(".armadactl")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	err = viper.MergeInConfig()

	if err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
			// This only occurs when looking for the default .armadactl file and it is not present
			// This is not an error as users don't have to specify it, so do nothing
		default:
			log.Errorf("Can't read config file %s because %s\n", viper.ConfigFileUsed(), err)
			os.Exit(1)
		}
	}
}

func ExtractCommandlineArmadaApiConnectionDetails() *ApiConnectionDetails {
	apiConnectionDetails := &ApiConnectionDetails{}
	viper.Unmarshal(apiConnectionDetails)
	return apiConnectionDetails
}
