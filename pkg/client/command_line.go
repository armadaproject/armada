package client

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
}

func LoadCommandlineArgsFromConfigFile(cfgFile string) error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error finding executable path: %s", err)
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
				return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
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
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error getting user home directory: %s", err)
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
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	}
	return nil
}

func ExtractCommandlineArmadaApiConnectionDetails() *ApiConnectionDetails {
	apiConnectionDetails := &ApiConnectionDetails{}
	viper.Unmarshal(apiConnectionDetails)
	return apiConnectionDetails
}
