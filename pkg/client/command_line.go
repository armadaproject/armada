package client

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func AddArmadaApiConnectionCommandlineArgs(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("armadaUrl", "localhost:50051", "specify armada server url")
	viper.BindPFlag("armadaUrl", rootCmd.PersistentFlags().Lookup("armadaUrl"))
}

// LoadCommandlineArgsFromConfigFile load config from at exePath/armadactl-defaults.yaml, where
// exePath is the path to the armadactl executable, the provided cfgFile, or, if it cfgFile="",
// $HOME/.armadactl
func LoadCommandlineArgsFromConfigFile(cfgFile string) error {

	// read config at exePath/armadactl-defaults.yaml, where exePath is the path to the armadactl
	// executable (if it exists)
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error finding executable path: %s", err)
	}

	exeDir := filepath.Dir(exePath)
	configPath := filepath.Join(exeDir, "/armadactl-defaults.yaml")
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore
		} else {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	}

	// if no cfgFile is provided, use $HOME/.armadactl
	if len(cfgFile) > 0 {
		viper.SetConfigFile(cfgFile)
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error getting user home directory: %s", err)
		}
		viper.AddConfigPath(homeDir)
		viper.SetConfigName(".armadactl")
	}

	// read environment variables
	viper.AutomaticEnv()

	// merge in new config with those loaded from armadactl-defaults.yaml
	// (note the call to viper.MergeInConfig instead of viper.ReadInConfig)
	if err := viper.MergeInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore
		} else {
			return fmt.Errorf("[LoadCommandlineArgsFromConfigFile] error reading config file %s: %s", viper.ConfigFileUsed(), err)
		}
	}

	return nil
}

// ExtractCommandlineArmadaApiConnectionDetails extracts Armada server connection details from the
// config loaded into viper. Hence, this function must be called after loading config into viper,
// e.g., by calling LoadCommandlineArgsFromConfigFile.
func ExtractCommandlineArmadaApiConnectionDetails() *ApiConnectionDetails {
	apiConnectionDetails := &ApiConnectionDetails{}
	viper.Unmarshal(apiConnectionDetails)
	return apiConnectionDetails
}
